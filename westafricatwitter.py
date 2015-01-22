"""
TREC DD 2015
Ebola domain

The goal is to generate ~1M high recall Tweets from West Africa in the last
9 months (from February 2014 to the end of November), during the onset of the
ebola crisis.

This first step identifies the _users_ tweeting from West Africa. Note that this
assumes the user has remained in place for the entire duration.
"""
import datetime
import dateutil
import dateutil.parser
import logging
from mrjob.job import MRJob
import mrjob.protocol as protocol
from mrjob.step import MRStep
import numpy as np
import os
import cPickle as pickle
import requests
from cStringIO import StringIO
from urllib2 import urlparse
# import zlib

# parse code
from twokenize import simpleTokenize
from trie import trie_append
from trie import trie_subseq

# ingest imports
from streamcorpus import decrypt_and_uncompress
from streamcorpus_pipeline._spinn3r_feed_storage import ProtoStreamReader


def init_gazetteers(filename):
        """
        Load newline-delimited gazetteer file at `filename` by
            - Tokenizing by whitespace
            - Loading n-grams into a trie.
        
        Pickles output
        """
        def preprocess_token(t):
            """Strip hashtags"""
            return t.lower().lstrip('#')

        trie = {}
        with open(filename) as f:
            for line in f:
                parts = [map(preprocess_token, x.split(' ')) + ['$']
                         for x in line.strip().split(',')]

                map(lambda x: trie_append(x, trie), parts)
        
        outname = filename + '.p' 
        with open(outname, 'wb') as out: 
            pickle.dump(trie, out)


class RawCSVProtocol(object):
    """
    Parses object as comma-separated values, with no quote escaping.
    :param object:
    """

    def read(self, line):
        """
        :param line:
        :return:
        """
        parts = line.split(',', 1)
        if len(parts) == 1:
            parts.append(None)

        return tuple(parts)

    def write(self, key, value):
        """
        Value is expected to be a string already.
        :param key:
        :param value:
        """
        vals = ','.join((key, value))
        return vals


class MRTwitterWestAfricaUsers(MRJob):
    """
    <Temporary empty docstring>
    """
    # INPUT_PROTOCOL = protocol.RawValueProtocol  # Custom parse tab-delimited values
    INTERNAL_PROTOCOL = protocol.PickleProtocol  # protocol.RawValueProtocol  # Serialize messages internally
    OUTPUT_PROTOCOL = RawCSVProtocol  # Output as csv

    def configure_options(self):
        """
        Configure default options needed by all jobs.
        Critically, each job needs a copy of the key to decrypt the tweets.
        """
        super(MRTwitterWestAfricaUsers, self).configure_options()
        self.add_file_option('--gpg-private',
                             default='trec-kba-2013-centralized.gpg-key.private',
                             help='path to gpg private key for decrypting the data')
        self.add_file_option('--west-africa-places',
                             default='westAfrica.csv.p',
                             help='path to pickled trie of west african places')
        self.add_file_option('--other-places',
                             default='otherPlace.csv.p',
                             help='path to pickled trie of places outside of'
                                  'west africa')
        self.add_file_option('--crisislex',
                             default='CrisisLexRec.csv.p',
                             help='path to pickled trie of crisislex terms')

    def steps(self):
        """
        :return list: The steps to be followed for the job
        """
        return [
            # Load files, getting tweets keyed to users
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper_get_tweet_per_user_from_files),
            # Get per-file stats
            MRStep(
                mapper_init=self.mapper_getter_init,
                mapper=self.mapper_get_user_stats_from_tweets,
                combiner=self.combiner_agg_stats_within_files,
                reducer=self.reducer_agg_stats_across_files)
        ]

    def mapper_init(self):
        """Set up a logger and initialize counters"""
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                            datefmt='%m-%d %H:%M',
                            filename='mrtwa.log',
                            filemode='w')
        self.logger = logging.getLogger(__name__)
        self.increment_counter('wa1', 'date_valid', 0)
        self.increment_counter('wa1', 'date_invalid', 0)

    def mapper_get_tweet_per_user_from_files(self, _, line):
        """
        Takes a line specifying a file in an s3 bucket,
        connects to and retrieves all tweets from the file.
        Tweets are _only_ yielded if they fall within the target date range
        (February 1, 2014 - November 30, 2014)
        :param _: the line number in the file listing the buckets (ignored)
        :param str|unicode line: pseudo-tab separated date, size amd file path
        :return tuple: user as key, language, post time, and body as tuple
        """

        feb_2014 = dateutil.parser.parse('2012-02-01 00:00:00+00:00')
        dec_2014 = dateutil.parser.parse('2012-12-01 00:00:00+00:00')

        aws_prefix, aws_path = line.strip().split()[-1].split('//')
        url = os.path.join('http://s3.amazonaws.com', aws_path)
        resp = requests.get(url)
        
        data = resp.content

        if not os.path.exists(self.options.gpg_private):
            self.logger.info('Cannot locate key: {}'.format(
                self.options.gpg_private))
            return

        errors, data = decrypt_and_uncompress(data, self.options.gpg_private)

        if errors:
            self.logger.info('\n'.join(errors))

        f = StringIO(data)
        reader = ProtoStreamReader(f)
        for entry in reader:
            # entries have other info, see other info here:
            #  https://github.com/trec-kba/streamcorpus-pipeline/blob/master/
            #       streamcorpus_pipeline/_spinn3r_feed_storage.py#L269
            tweet = entry.feed_entry

            tweet_time = dateutil.parser.parse(tweet.last_published)
            user_link = tweet.author[0].link[0].href
            user = urlparse.urlsplit(user_link).path[1:]
            body = tweet.title.encode('utf8')
            lang = tweet.lang[0].code

    def load_gazetteers(self, filename):
        with open(filename, 'rb') as f:
            return pickle.load(f)
            if feb_2014 <= tweet_time <= dec_2014:
                self.increment_counter('wa1', 'date_valid', 1)
                yield (user, (tweet_time, body, lang))
            else:
                self.increment_counter('wa1', 'date_invalid', 1)
                self.logger.debug('Bad time:{}'.format(tweet_time))
                # yield (user, None)

    def mapper_getter_init(self):
        """Initialize variables used in getting mapper data"""
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                            datefmt='%m-%d %H:%M',
                            filename='/tmp/mrtwa.log',
                            filemode='a')
        self.logger = logging.getLogger(__name__)
        self.increment_counter('wa1', 'line_invalid', 0)

        self.utc_7 = datetime.time(7, 0, 0)

        self.west_africa_places = self.load_gazetteers(self.options.west_africa_places)
        self.other_places = self.load_gazetteers(self.options.other_places)

        self.crisislex_grams = self.load_gazetteers(self.options.crisislex)

    def mapper_get_user_stats_from_tweets(self, user, tweet_tuple):
        """
        Discards tweets before February 2014 and after November 2014.
        West Africa time is considered to be 0 to +1 UTC.
        7AM to 11PM are taken as daylight hours.
        :param str|unicode user: the username
        :param tuple tweet_tuple: time, body, and language
        :return tuple:
            username,
                (
                count                    # 1
                is_in_west_africa_time,  # 0 or 1
                west_africa_loc_mention, # 0 or 1
                other_loc_mention,       # 0 or 1
                crisislex_mention        # 0 or 1
                ebola_mention            # 0 or 1
                time                     # datetime
                )
        """
        try:
            tweet_time, body, lang = tweet_tuple
        except ValueError:
            self.increment_counter('wa1', 'line_invalid', 1)
            self.logger.debug('Got ValueError:{}'.format(tweet_tuple))
            return

        ###
        # Meta-data features
        ###

        ############################################
        # Is the tweet after 7 AM?
        ############################################
        is_in_time = int(datetime.time(tweet_time.hour,
                                       tweet_time.minute,
                                       tweet_time.second) >= self.utc_7)

        ###
        # Text Features
        ###

        # tokenize tweet
        tweet_tokens = simpleTokenize(body)

        ############################################
        # Does the tweet mention places?
        ############################################
        west_africa_mention = trie_subseq(tweet_tokens, self.west_africa_places)
        other_place_mention = trie_subseq(tweet_tokens, self.other_places)

        ############################################
        # Does the tweet mention keywords or topics related to medicine/Ebola?
        ############################################
        # TODO: Mine Ebola and medical synsets from wordnet
        # TODO: Mine Ebola and medical terms from NELL - weak results so far
        # Currently just opting for 'ebola'. More might not be worth it.
        ebola_mention = 1 if 'ebola' in tweet_tokens else 0

        ############################################
        # Does the tweet contain keywords related to CrisisLex disasters
        ############################################
        crisislex_mention = trie_subseq(tweet_tokens, self.crisislex_grams)

        ############################################
        # Was the tweet made by an account associated with the disaster?
        # Define list based on a first pass that sees how many tweets
        # related to the topic each user makes, choose a threshold.
        # Define list based on known accounts, such as CDC, doctors
        # without borders, etc.
        ############################################

        ############################################
        # return result
        ############################################
        yield user, (1,
                     is_in_time,
                     west_africa_mention,
                     other_place_mention,
                     crisislex_mention,
                     ebola_mention)

    def combiner_agg_stats_within_files(self, user, tweet_tuples):
        """
        :param str|unicode user: The user who made the tweets
        :param tuple:
                    1,
                    is_in_time,
                    west_africa_mention,
                    other_place_mention,
                    crisislex_mention,
                    ebola_mention
                    (this is a tuple generator)
        :return tuple: merge all results within the file
        """
        yield user, np.apply_along_axis(sum, 0, list(tweet_tuples))

    def reducer_agg_stats_across_files(self, user, tuples_over_file):
        """
        :param str|unicode user: The user who made the tweet
        :param np.array tuples_over_file:
        :return tuple:
        """
        tuples_over_files = np.apply_along_axis(sum, 0, list(tuples_over_file))
        count, is_in_time, \
        west_africa_mention, other_place_mention, \
        crisislex_mention, ebola_mention = tuples_over_files
        if 1.*is_in_time/count >= 0.75 \
                and west_africa_mention > other_place_mention:
            yield user, ','.join([str(x) for x in tuples_over_files])


if __name__ == '__main__':
    # Set up tries
    for fname in ['westAfrica.csv', 'otherPlace.csv', 'CrisisLexRec.csv']:
        if not os.path.exists(fname+'.p'):
            init_gazetteers(fname)

    # Start Map Reduce Job
    MRTwitterWestAfricaUsers.run()
