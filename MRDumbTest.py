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
from mrjob.job import MRJob
from mrjob.protocol import PickleProtocol
import logging
import os
import sys

from RawCSVProtocol import RawCSVProtocol
from twokenize import simpleTokenize


def get_tokens_sans_hashmarks(tweet_data):
    tweet_tokens = [tok.lower() for tok in simpleTokenize(tweet_data.title.encode('utf8'))]
    return [tok.lstrip('#') for tok in tweet_tokens]


class MRDumbTest(MRJob):
    """
    <Temporary empty docstring>
    """
    # INPUT_PROTOCOL = protocol.RawValueProtocol  # Custom parse tab-delimited values
    INTERNAL_PROTOCOL = PickleProtocol  # protocol.RawValueProtocol  # Serialize messages internally
    OUTPUT_PROTOCOL = RawCSVProtocol  # Output as csv

    def configure_options(self):
        """
        Configure default options needed by all jobs.
        Each job _must_have_ a copy of the key to decrypt the tweets.
        """
        super(MRDumbTest, self).configure_options()
        self.add_file_option('--gpg-private',
                             default='trec-kba-2013-centralized.gpg-key.private',
                             help='path to gpg private key for decrypting the data')
        self.add_file_option('--west-africa-places',
                             default='only_west_africa.csv.p',
                             help='path to list of locations in West Africa.')

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
        self.increment_counter('wa1', 'geocoded', 0)
        self.increment_counter('wa1', 'waf_mention', 0)
        self.increment_counter('wa1', 'ebola_mention', 0)
        self.increment_counter('wa1', 'duplicate_entries', 0)

        if not os.path.exists(self.options.gpg_private):
            self.logger.info('Cannot locate key: {}'.format(
                self.options.gpg_private))
            sys.exit(1)

    def mapper(self, _, line):
        """
        A nothing
        :param _:
        :param line:
        :return:
        """
        yield _, line

if __name__ == '__main__':
    MRDumbTest.run()