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
# import codecs
import logging
import os
from cStringIO import StringIO
from urllib2 import urlparse

from mrjob.job import MRJob
from mrjob.protocol import PickleProtocol
from mrjob.protocol import RawValueProtocol
import requests
import sys

# parse code
from twokenize import simpleTokenize

# ingest imports
from streamcorpus import decrypt_and_uncompress
from streamcorpus_pipeline._spinn3r_feed_storage import ProtoStreamReader


class MRGetUsersUsingKeywords(MRJob):
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
        super(MRGetUsersUsingKeywords, self).configure_options()
        self.add_file_option('--gpg-private',
                             default='trec-kba-2013-centralized.gpg-key.private',
                             help='path to gpg private key for decrypting the data')
        self.add_file_option('--keyword-file',
                             default='seed_keywords.csv',
                             help='path to list of keywords')

    def mapper_init(self):
        """Set up a logger, counters, and a set of keywords"""
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                            datefmt='%m-%d %H:%M',
                            filename='./mrtwa.log',
                            filemode='w')
        self.logger = logging.getLogger(__name__)
        self.increment_counter('wa1', 'file_date_valid', 0)
        self.increment_counter('wa1', 'file_data_bad', 0)
        self.increment_counter('wa1', 'tweet_date_valid', 0)
        self.increment_counter('wa1', 'tweet_date_invalid', 0)
        self.increment_counter('wa1', 'spam_count', 0)

        if not os.path.exists(self.options.gpg_private):
            self.logger.info('Cannot locate key: {}'.format(
                self.options.gpg_private))
            sys.exit(1)

        self.keywords = [x.strip() for x in open(self.options.keyword_file, 'r')]

    def mapper(self, _, line):
        """
        Takes a line specifying a file in an s3 bucket,
        connects to and retrieves all tweets from the file.
        Yields summary stats totaling up the number of time each keyword is used.
        :param _: the line number in the file listing the buckets (ignored)
        :param str|unicode line: pseudo-tab separated date, size amd file path
        :return tuple: user as key, language, post time, and body as tuple
        """
        cur_line = line.strip()
        aws_prefix, aws_path = cur_line.split('//')
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
        try:
            for entry in reader:
                # entries have other info, see other info here:
                # https://github.com/trec-kba/streamcorpus-pipeline/blob/master/
                #       streamcorpus_pipeline/_spinn3r_feed_storage.py#L269
                tweet = entry.feed_entry
                if tweet.spam_probability > 0.5:
                    self.increment_counter('wa1', 'spam_count', 1)
                    continue

                try:
                    user_link = tweet.author[0].link[0].href
                    user_scrn_uni = urlparse.urlsplit(user_link).path.split('@')[1].lower()
                    # user_name_scrn_uni = tweet.author[0].name
                    # user_name_uni = ''.join(user_name_scrn_uni.split(' (')[1:])[:-1].lower()
                    # user_scrn_uni = user_name_scrn_uni.split(' (')[0]

                    # When tokenizing, strip hashmarks from hashtags
                    tokens = set([x[1:] if x[0] == '#' else x for x in simpleTokenize(tweet.title.lower())])

                    yield (user_scrn_uni.encode('utf8'), [1] + [1 if x in tokens else 0 for x in self.keywords])

                except Exception as e:
                    self.increment_counter('line_exception', type(e).__name__, 1)
        except Exception as e:
            self.increment_counter('file_exception', type(e).__name__, 1)

    def combiner(self, user, tweet_tuples):
        """
        Combine all tuples within a file.
        A small hack for speed: tuples are _only_ yielded *if* they have at least
        one value greated than zero.
        This means we get an undercount of the user's total volume of tweets!
        :param str|unicode user: The user who made the tweets
        :param tweet_tuples:
                    1,
                    (all other keywords in the list)
        :return tuple: user, sum of all results (*including* times)
        """
        tuples_over_file = map(sum, zip(*tweet_tuples))
        is_nonzero = False
        for val in tuples_over_file[1:]:
            if val > 0:
                is_nonzero = True
                break

        if is_nonzero:
            yield user, tuples_over_file

    def reducer(self, user, tuples_over_file):
        """
        We *only* yield results for users with nonzero results. (See combiner.)
        :param str|unicode user: The user who made the tweet
        :param tuple tuples_over_file: aggregated tweet tuples fromm the combiner
        :return tuple:
        """
        tuples_over_files = map(sum, zip(*tuples_over_file))
        yield None, user+','+','.join([str(x) for x in tuples_over_files])


if __name__ == '__main__':
    # Set up tries
    # for fname in ['only_west_africa.csv', 'only_other_places.csv', 'CrisisLexRec.csv']:
    #    if not os.path.exists(fname + '.tr'):
    #        write_gazetteer_to_trie_pickle_file(fname)

    # Start Map Reduce Job
    MRGetUsersUsingKeywords.run()
