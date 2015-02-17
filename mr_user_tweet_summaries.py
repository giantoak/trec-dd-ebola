# -*- coding: utf-8 -*-
__author__ = 'Sam Zhang, Peter M. Landwehr'
"""
TREC DD 2015
Ebola domain

The goal is to generate ~1M high recall Tweets from West Africa in the last
10 months (from February 2014 to the end of November), during the onset of the
ebola crisis.

This first step identifies the _users_ tweeting from West Africa. Note that this
assumes the user has remained in place for the entire duration.
"""
from cStringIO import StringIO
# import datetime
import dateutil
import dateutil.parser
import logging
import os
# import cPickle as pickle
from mrjob.job import MRJob
import mrjob.protocol as protocol
from mrjob.step import MRStep
import requests
from streamcorpus import decrypt_and_uncompress
from streamcorpus_pipeline._spinn3r_feed_storage import ProtoStreamReader
import sys
from urllib2 import urlparse
from twokenize import simpleTokenize
from sam_trie import trie_subseq
from sam_trie import load_trie_from_pickle_file
# from sam_trie import write_gazetteer_to_trie_pickle_file


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


def get_tokens_sans_hashmarks(tweet_data):
    tweet_tokens = [tok.lower() for tok in simpleTokenize(tweet_data.title.encode('utf8'))]
    return [tok.lstrip('#') for tok in tweet_tokens]


class MRUserTweetSummaries(MRJob):
    """
    <Temporary empty docstring>
    """
    # INPUT_PROTOCOL = protocol.RawValueProtocol  # Custom parse tab-delimited values
    INTERNAL_PROTOCOL = protocol.PickleProtocol  # protocol.RawValueProtocol  # Serialize messages internally
    OUTPUT_PROTOCOL = RawCSVProtocol  # Output as csv

    def configure_options(self):
        """
        Configure default options needed by all jobs.
        Each job _must_have_ a copy of the key to decrypt the tweets.
        """
        super(MRUserTweetSummaries, self).configure_options()
        self.add_file_option('--gpg-private',
                             default='trec-kba-2013-centralized.gpg-key.private',
                             help='path to gpg private key for decrypting the data')
        self.add_file_option('--west-africa-places',
                             default='only_west_africa.csv.p',
                             help='path to list of locations in West Africa.')
        self.add_file_option('--crisislex',
                             default='CrisisLexRec.csv.p',
                             help='path to pickled trie of crisislex terms')

    def steps(self):
        """
        :return list: The steps to be followed for the job
        """
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper_get_user_tweet_characteristics(),
                reducer=self.reducer_get_merged_characteristics)
        ]

    def mapper_init(self):
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
        self.increment_counter('wa1', 'not_twitter', 0)
        self.increment_counter('wa1', 'likely_spam', 0)
        self.increment_counter('wa1', 'tweet_date_valid', 0)
        self.increment_counter('wa1', 'tweet_date_invalid', 0)
        self.increment_counter('wa1', 'waf_mention', 0)
        self.increment_counter('wa1', 'ebola_mention', 0)
        self.increment_counter('wa1', 'lex_mention', 0)

        if not os.path.exists(self.options.gpg_private):
            self.logger.info('Cannot locate key: {}'.format(
                self.options.gpg_private))
            sys.exit(1)

        self.naive_feb_2014 = dateutil.parser.parse('2014-02-01')
        self.naive_dec_2014 = dateutil.parser.parse('2014-12-01')
        self.feb_2014 = dateutil.parser.parse('2014-02-01 00:00:00+00:00')
        self.dec_2014 = dateutil.parser.parse('2014-12-01 00:00:00+00:00')

        self.west_africa_places = load_trie_from_pickle_file(self.options.west_africa_places)
        self.crisislex = load_trie_from_pickle_file(self.options.crisislex)


    def mapper_get_user_tweet_characteristics(self, _, line):
        """
        Takes a line specifying a file in an s3 bucket. IF it's in the date range,
        retrieve tweets from the file. Yield _tweets_ that:
          * have geotags in West Africa
          * use language that specifically mentions West African geographic features
          * have a profile in the West African time zone and are using an expected
            language for that time zone.
        connects to and retrieves all tweets from the file
        Tweets are _only_ yielded if they fall within the target date range
        (February 1, 2014 - November 30, 2014)
        Returned tweets are broken down based on:
         * if they are in West Africa
         * if they mention West African features
         ....
        :param _: the line number in the file listing the buckets (ignored)
        :param str|unicode line: pseudo-tab separated date, size amd file path
        :return tuple: user as key, language, post time, and body as tuple
        """
        aws_prefix, aws_path = line.strip().split()[-1].split('//')
        bucket_date = dateutil.parser.parse(aws_path.split('/')[-2])
        if bucket_date < self.naive_feb_2014 or bucket_date > self.naive_dec_2014:
            self.increment_counter('wa1', 'file_date_invalid', 1)
            return

        self.increment_counter('wa1', 'file_date_valid', 1)
        url = os.path.join('http://s3.amazonaws.com', aws_path)
        resp = requests.get(url)

        data = resp.content
        if data is None:
            self.logger('{}: did not retrieve any data. Skipping...\n'.format(aws_path))
            self.increment_counter('wa1', 'file_data_bad', 1)
            return

        errors, data = decrypt_and_uncompress(data, self.options.gpg_private)
        if errors:
            self.logger.info('\n'.join(errors))
        if data is None:
            self.logger('{}: did not decrypt any data. Skipping...\n'.format(aws_path))
            self.increment_counter('wa1', 'file_data_bad', 1)
            return

        f = StringIO(data)
        reader = ProtoStreamReader(f)
        for entry in reader:
            # entries have other info, see other info here:
            #  https://github.com/trec-kba/streamcorpus-pipeline/blob/master/
            #       streamcorpus_pipeline/_spinn3r_feed_storage.py#L269
            tweet_data = entry.feed_entry
            tweet_url = tweet_data.canonical_link.href

            # We skip all entries that aren't tweets.
            if tweet_url[:19] != 'http://twitter.com/':
                self.increment_counter('wa1', 'not_twitter', 1)
                continue

            # We skip all entries that are likely to be spam.
            if tweet_data.spam_probability > 0.5:
                self.increment_counter('wa1', 'likely_spam', 1)
                continue

            # We skip all tweets that are outside of the date range
            tweet_time = dateutil.parser.parse(tweet_data.last_published)
            if tweet_time < self.feb_2014 or tweet_time >= self.dec_2014:
                self.increment_counter('wa1', 'tweet_date_invalid', 1)
                continue

            self.increment_counter('wa1', 'tweet_date_valid', 1)

            # Get data
            tokens_no_hashtags = get_tokens_sans_hashmarks(tweet_data)
            user_link = tweet_data.author[0].link[0].href
            user = urlparse.urlsplit(user_link).path[1:]
            lang = tweet_data.lang[0].code

            features = []
            # If the tweet mentions a location that is _only_ found in West
            # Africa, we'll log it.
            if trie_subseq(tokens_no_hashtags, self.west_africa_places):
                self.increment_counter('wa1', 'waf_mention', 1)
                features.append('waf')
                yield (user, 'waf_mention')

            # If the tweet mentions 'ebola', we'll log it.
            if 'ebola' in tokens_no_hashtags:
                features.append('ebola')
                self.increment_counter('wa1', 'ebola_mention', 1)

            # If the tweet uses a crisislex phrase, we'll log it.
            if trie_subseq(tokens_no_hashtags, self.crisislex_grams):
                features.append('lex')
                self.increment_counter('lex_mention', 1)

            if len(features) == 0:
                yield user,  'none'
            elif len(features) == 1:
                yield user, features[0]
            elif len(features) == 2:
                yield user, ''.join(features)
            else:
                yield user, 'all'


    def reducer_get_merged_characteristics(self, user, tweet_types):
        """
        Combine all of the noted tweet features to get summary stats for the user.
        :param user:
        :param tweet_types:
        :return:
        """
        temp_dict = {'waf': 0, 'ebola': 0,
                     'wafebola': 0, 'waflex': 0, 'ebolalex': 0,
                     'all': 0, 'none': 0}
        for tweet_type in tweet_types:
            temp_dict[tweet_type] += 1

        dict_strs = [str(temp_dict[x]) for x in ['waf', 'ebola',
                                                 'wafebola', 'waflex', 'ebolalex',
                                                 'all', 'none']]
        yield user+','+','.join(dict_strs)

if __name__ == '__main__':
    # Start Map Reduce Job
    MRUserTweetSummaries.run()




