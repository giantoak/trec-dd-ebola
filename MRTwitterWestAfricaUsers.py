# -*- coding: utf-8 -*-
__author__ = 'Sam Zhang, Peter M. Landwehr'
"""
TREC DD 2015
Ebola domain

The goal is to generate ~1M high recall Tweets from West Africa in the last
10 months (from February 2014 to the end of November), during the onset of the
ebola crisis.

This program extracts users who appear to mention west africa a moderate number of times
and who on average tweet between 10 AM and 8 PM in UTC 0 (west african time).
"""
import codecs
import datetime
import logging
import os
from cStringIO import StringIO
from urllib2 import urlparse

import dateutil
import dateutil.parser
# from mrjob.emr import S3Filesystem
from mrjob.job import MRJob
from mrjob.protocol import PickleProtocol
from mrjob.protocol import RawValueProtocol
from mrjob.step import MRStep
import requests
import sys

# parse code
# from RawCSVProtocol import RawCSVProtocol
# from sam_trie import trie_subseq
# from sam_trie import load_trie_from_pickle_file
# from sam_trie import write_gazetteer_to_trie_pickle_file
from twokenize import simpleTokenize
import marisa_trie

# ingest imports
from streamcorpus import decrypt_and_uncompress
from streamcorpus_pipeline._spinn3r_feed_storage import ProtoStreamReader


def write_gazetteer_to_trie_pickle_file(filename):
    """
    Load newline-delimited gazetteer file at `filename` by
        - Tokenizing by whitespace
        - Loading n-grams into a trie.

    Pickles output
    """
    with codecs.open(filename, 'r', 'utf8') as infile:
        lines = list(set(line.lower().strip() for line in infile))

    trie = marisa_trie.Trie(lines)
    trie.save(filename + '.tr')


def load_trie_from_pickle_file(filename):
    trie = marisa_trie.Trie()
    trie.load(filename)
    return trie


def any_word_subsequence_in_trie(tweet_tokens, trie):
    """
    :param list|tuple tweet_tokens: tokenized tweet
    :param marisa_trie.Trie: trie
    :return bool: True if some subset of the tweet's words are in trie, else False
    """
    if len(tweet_tokens) == 0:
        return False
    cur_uni = tweet_tokens[-1]
    if len(trie.prefixes(cur_uni)) > 0:
        return True
    for i in xrange(len(tweet_tokens) - 2, -1, -1):
        cur_uni = tweet_tokens[i] + ' ' + cur_uni
        if len(trie.prefixes(cur_uni)) > 0:
            return True
    return False


class MRTwitterWestAfricaUsers(MRJob):
    """
    <Temporary empty docstring>
    """
    # INPUT_PROTOCOL = protocol.RawValueProtocol  # Custom parse tab-delimited values
    INTERNAL_PROTOCOL = PickleProtocol  # protocol.RawValueProtocol  # Serialize messages internally
    OUTPUT_PROTOCOL = RawValueProtocol  # Output as csv

    def configure_options(self):
        """
        Configure default options needed by all jobs.
        Each job _must_have_ a copy of the key to decrypt the tweets.
        """
        super(MRTwitterWestAfricaUsers, self).configure_options()
        self.add_file_option('--gpg-private',
                             default='trec-kba-2013-centralized.gpg-key.private',
                             help='path to gpg private key for decrypting the data')
        self.add_file_option('--west-africa-places',
                             default='only_west_africa.csv.tr',
                             help='path to pickled trie of west african places')
        self.add_file_option('--other-places',
                             default='only_other_places.csv.tr',
                             help='path to pickled trie of non-west african places')
        self.add_file_option('--crisislex',
                             default='CrisisLexRec.csv.tr',
                             help='path to pickled trie of crisislex terms')

    def steps(self):
        """
        :return list: The steps to be followed for the job
        """
        return [
            # Load files, getting tweets in date range
            # MRStep(
            # mapper=self.get_tweets_in_date_range_from_files),
            # Load files, getting tweets keyed to users
            MRStep(
                mapper_init=self.mapper_get_tweets_init,
                mapper=self.mapper_get_tweets_per_user_in_date_range_from_files),
            # Get per-file stats
            MRStep(
                mapper_init=self.mapper_get_user_init,
                mapper=self.mapper_get_user_stats_from_tweets,
                combiner=self.combiner_agg_stats_within_files,
                reducer=self.reducer_agg_stats_across_files)
        ]

    def mapper_get_tweets_init(self):
        """Set up a logger and initialize counters"""
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                            datefmt='%m-%d %H:%M',
                            filename='./mrtwa.log',
                            filemode='w')
        self.logger = logging.getLogger(__name__)
        self.increment_counter('wa1', 'file_date_invalid', 0)
        self.increment_counter('wa1', 'file_date_valid', 0)
        self.increment_counter('wa1', 'file_data_bad', 0)
        self.increment_counter('wa1', 'tweet_date_valid', 0)
        self.increment_counter('wa1', 'tweet_date_invalid', 0)
        self.increment_counter('wa1', 'spam_count', 0)

        if not os.path.exists(self.options.gpg_private):
            self.logger.info('Cannot locate key: {}'.format(
                self.options.gpg_private))
            sys.exit(1)

        self.naive_feb_2014 = dateutil.parser.parse('2014-02-01')
        self.naive_dec_2014 = dateutil.parser.parse('2014-12-01')
        self.feb_2014 = dateutil.parser.parse('2014-02-01 00:00:00+00:00')
        self.dec_2014 = dateutil.parser.parse('2014-12-01 00:00:00+00:00')

    def mapper_get_tweets_per_user_in_date_range_from_files(self, _, line):
        """
        Takes a line specifying a file in an s3 bucket,
        connects to and retrieves all tweets from the file.
        Tweets are _only_ yielded if they fall within the target date range
        (February 1, 2014 - November 30, 2014)
        Returned tweets are broken down based oni f they mention
        West African features
        :param _: the line number in the file listing the buckets (ignored)
        :param str|unicode line: pseudo-tab separated date, size amd file path
        :return tuple: user as key, language, post time, and body as tuple
        """
        cur_line = line.strip()
        aws_prefix, aws_path = cur_line.split('//')
        file_date = dateutil.parser.parse(aws_path.split('/')[-2])

        file_date_okay = False
        try:
            file_date_okay = self.naive_feb_2014 <= file_date < self.naive_dec_2014
        except TypeError:
            # Assume that this is caused by naive date times
            try:
                file_date_okay = self.feb_2014 <= file_date < self.dec_2014
            except:
                self.increment_counter('wa1', 'file_date_exception', 1)

        if not file_date_okay:
            self.increment_counter('wa1', 'file_date_invalid', 1)
            return

        self.increment_counter('wa1', 'file_date_valid', 1)
        url = os.path.join('http://s3.amazonaws.com', aws_path)
        resp = requests.get(url)

        data = resp.content
        if data is None:
            self.logger.info('{}: did not retrieve any data. Skipping...\n'.format(aws_path))
            self.increment_counter('wa1', 'file_data_bad', 1)
            return

        if not os.path.exists(self.options.gpg_private):
            self.logger.info('Cannot locate key: {}'.format(self.options.gpg_private))
            self.increment_counter('wa1', 'missing_key', 1)
            return

        errors, data = decrypt_and_uncompress(data, self.options.gpg_private)

        if errors:
            self.logger.info('\n'.join(errors))
        if data is None:
            self.logger.info('{}: did not decrypt any data. Skipping...\n'.format(aws_path))
            self.increment_counter('wa1', 'file_data_bad', 1)
            return

        f = StringIO(data)
        reader = ProtoStreamReader(f)
        for entry in reader:
            # entries have other info, see other info here:
            # https://github.com/trec-kba/streamcorpus-pipeline/blob/master/
            #       streamcorpus_pipeline/_spinn3r_feed_storage.py#L269
            tweet = entry.feed_entry

            tweet_time = dateutil.parser.parse(tweet.last_published)
            tweet_time_okay = False
            try:
                tweet_time_okay = self.feb_2014 <= tweet_time < self.dec_2014
            except TypeError:
                # Assume that this is caused by naive date times
                try:
                    tweet_time_okay = self.naive_feb_2014 <= tweet_time < self.naive_dec_2014
                except:
                    self.increment_counter('wa1', 'tweet_date_exception', 1)

            if not tweet_time_okay:
                self.increment_counter('wa1', 'tweet_date_invalid', 1)
                self.logger.debug('Bad time:{}'.format(tweet_time))
                continue
            self.increment_counter('wa1', 'tweet_date_valid', 1)

            if tweet.spam_probability > 0.5:
                self.increment_counter('wa1', 'spam_count', 1)
                continue

            try:
                user_link = tweet.author[0].link[0].href
                user_scrn_uni = urlparse.urlsplit(user_link).path.split('@')[1].lower()
                user_name_scrn_uni = tweet.author[0].name
                user_name_uni = ''.join(user_name_scrn_uni.split(' (')[1:])[:-1].lower()
                # user_scrn_uni = user_name_scrn_uni.split(' (')[0]

                body_uni = tweet.title
                lang = tweet.lang[0].code

                yield (user_scrn_uni.encode('utf8'), (tweet_time, body_uni, user_name_uni, lang))

            except:
                self.increment_counter('wa1', 'other_exception', 1)

    def mapper_get_user_init(self):
        """Initialize variables used in getting mapper data"""
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                            datefmt='%m-%d %H:%M',
                            filename='./mrtwa.log',
                            filemode='a')
        self.logger = logging.getLogger(__name__)
        self.increment_counter('wa1', 'line_invalid', 0)
        self.increment_counter('wa1', 'line_valid', 0)

        self.utc_7 = datetime.time(7, 0, 0)

        self.west_africa_places = load_trie_from_pickle_file(self.options.west_africa_places)
        self.other_places = load_trie_from_pickle_file(self.options.other_places)

        self.crisislex_grams = load_trie_from_pickle_file(self.options.crisislex)

    def mapper_get_user_stats_from_tweets(self, user, tweet_tuple):
        """
        Discards tweets before February 2014 and after November 2014.
        West Africa time is considered to be 0 to +1 UTC.
        7AM to 11PM are taken as daylight hours.
        :param str|unicode user: the username
        :param tuple tweet_tuple: time, body, full user name, and language
        :return tuple:
            username,
                (
                count                    # 1
                is_after_7AM,            # 0 or 1
                west_africa_loc_mention, # 0 or 1
                other_loc_mention,       # 0 or 1
                crisislex_mention        # 0 or 1
                ebola_mention            # 0 or 1
                time_of_day_in_seconds   # int
                name_mentions_w_africa   # 0 or 1
                )
        """
        try:
            tweet_time, body_uni, user_name_uni, lang = tweet_tuple
        except ValueError:
            self.increment_counter('wa1', 'line_invalid', 1)
            self.logger.debug('Got ValueError:{}'.format(tweet_tuple))
            return

        self.increment_counter('wa1', 'line_valid', 1)

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
        tweet_tokens = simpleTokenize(body_uni)
        # mentions = [tok[1:] for tok in tokens if len(tok) > 1 and tok[0] == '@']

        ############################################
        # Does the tweet mention places in west africa?
        ############################################
        west_africa_mention = \
            int(any_word_subsequence_in_trie(tweet_tokens,
                                             self.west_africa_places))

        other_place_mention = \
            int(any_word_subsequence_in_trie(tweet_tokens,
                                             self.other_places))

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
        crisislex_mention = \
            int(any_word_subsequence_in_trie(tweet_tokens,
                                             self.crisislex_grams))

        ############################################
        # Does the user have one of the three afflicted nations
        # in its username?
        ############################################
        name_mentions_west_africa = 0
        toks = user_name_uni.replace(',', ' ').replace('.', ' ').lower().split()
        for country in ['liberia', 'guinea']:
            if country in toks:
                name_mentions_west_africa = 1
        if user_name_uni.lower().find('sierra leone') > -1:
            name_mentions_west_africa = 1

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
                     ebola_mention,
                     tweet_time.hour * 3600 + tweet_time.minute * 60 + tweet_time.second,
                     name_mentions_west_africa)

    def combiner_agg_stats_within_files(self, user, tweet_tuples):
        """
        :param str|unicode user: The user who made the tweets
        :param tweet_tuples:
                    1,
                    is_in_time,
                    west_africa_mention,
                    other_place_mention,
                    crisislex_mention,
                    ebola_mention,
                    time_in_seconds
                    name_mentions_west_africa
                    (this is a tuple generator)
        :return tuple: user, sum of all results (*including* times)
        """
        yield user, map(sum, zip(*tweet_tuples))

    def reducer_agg_stats_across_files(self, user, tuples_over_file):
        """
        :param str|unicode user: The user who made the tweet
        :param tuple tuples_over_file:
        :return tuple:
        """
        tuples_over_files = map(sum, zip(*tuples_over_file))

        # Swap mean time for total time
        count, is_in_time, west_africa_mention, other_place_mention, crisislex_mention, ebola_mention, total_time, name_mentions_west_africa = tuples_over_files
        mean_time = 1. * total_time / count
        tuples_over_files = count, is_in_time, west_africa_mention, other_place_mention, crisislex_mention, ebola_mention, mean_time, name_mentions_west_africa

        # Yield users whose names include West African Countries most of the time.
        if 1. * name_mentions_west_africa / count > 0.5:
            yield None, user+','+','.join([str(x) for x in tuples_over_files])

        # Yield users with at least 10 tweets
        # who mention West African locations at least three times
        if count > 9 and west_africa_mention > 3:
            yield None, user+','+','.join([str(x) for x in tuples_over_files])


if __name__ == '__main__':
    # Set up tries
    # for fname in ['only_west_africa.csv', 'only_other_places.csv', 'CrisisLexRec.csv']:
    #    if not os.path.exists(fname + '.tr'):
    #        write_gazetteer_to_trie_pickle_file(fname)

    # Start Map Reduce Job
    MRTwitterWestAfricaUsers.run()
