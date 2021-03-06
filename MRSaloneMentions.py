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
import logging
import os
from cStringIO import StringIO

from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
import requests
import sys

from twokenize import simpleTokenize

# ingest imports
from streamcorpus import decrypt_and_uncompress
from streamcorpus_pipeline._spinn3r_feed_storage import ProtoStreamReader


class MRSaloneMentions(MRJob):
    """
    <Temporary empty docstring>
    """
    # INPUT_PROTOCOL = protocol.RawValueProtocol  # Custom parse tab-delimited values
    # INTERNAL_PROTOCOL = PickleProtocol  # protocol.RawValueProtocol  # Serialize messages internally
    OUTPUT_PROTOCOL = RawValueProtocol  # Output as csv

    def configure_options(self):
        """
        Configure default options needed by all jobs.
        Each job _must_have_ a copy of the key to decrypt the tweets.
        """
        super(MRSaloneMentions, self).configure_options()
        self.add_file_option('--gpg-private',
                             default='trec-kba-2013-centralized.gpg-key.private',
                             help='path to gpg private key for decrypting the data')

    def mapper_init(self):
        """Set up a logger and initialize counters"""
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                            datefmt='%m-%d %H:%M',
                            filename='./mrtwa.log',
                            filemode='w')
        self.logger = logging.getLogger(__name__)
        self.increment_counter('wa1', 'file_data_bad', 0)
        self.increment_counter('wa1', 'spam_count', 0)
        self.increment_counter('wa1', 'valid_tweets', 0)

        if not os.path.exists(self.options.gpg_private):
            self.logger.info('Cannot locate key: {}'.format(
                self.options.gpg_private))
            sys.exit(1)

    def mapper(self, _, line):
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

            if tweet.spam_probability > 0.5:
                self.increment_counter('wa1', 'spam_count', 1)
                continue

            try:

                body_uni = tweet.title
                tokens = [x[1:] if x[0] == '#' else x for x in simpleTokenize(body_uni.lower().encode('utf8'))]
                if 'saloneindependence' in tokens:
                    yield None, entry
                elif 'saloneindependance' in tokens:
                    yield None, entry
                elif 'salone' in tokens:
                    yield None, entry
                elif 'sierraleone' in tokens:
                    yield None, entry

                self.increment_counter('wa1', 'valid_tweets', 1)

            except:
                self.increment_counter('wa1', 'other_exception', 1)



if __name__ == '__main__':
    # Set up tries
    # for fname in ['only_west_africa.csv', 'only_other_places.csv', 'CrisisLexRec.csv']:
    #    if not os.path.exists(fname + '.tr'):
    #        write_gazetteer_to_trie_pickle_file(fname)

    # Start Map Reduce Job
    MRSaloneMentions.run()
