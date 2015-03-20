# -*- coding: utf-8 -*-
__author__ = 'Sam Zhang, Peter M. Landwehr'
"""
TREC DD 2015
Ebola domain

The goal is to generate ~1M high recall Tweets from West Africa in the last
10 months (from February 2014 to the end of November), during the onset of the
ebola crisis.

This program filters a corpus of Tweets for ones made by someone on a list of Twitter User IDs. 

"""
import logging
import os
from cStringIO import StringIO

from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
import requests
import sys

# ingest imports
from streamcorpus import decrypt_and_uncompress
from streamcorpus_pipeline._spinn3r_feed_storage import ProtoStreamReader


class MRUsersToTweets(MRJob):
    """
    This program filters a corpus of Tweets for ones made by someone on a list of Twitter User IDs. 
    """
    OUTPUT_PROTOCOL = RawValueProtocol  # Output as csv

    def configure_options(self):
        """
        Configure default options needed by all jobs.
        Each job _must_have_ a copy of the key to decrypt the tweets.
        """
        super(MRUsersToTweets, self).configure_options()
        self.add_file_option('--gpg-private',
                             default='trec-kba-2013-centralized.gpg-key.private',
                             help='path to gpg private key for decrypting the data')
        self.add_file_option('--desired-users',
                default='seed_usernames.csv',
                help='path to list of desired usernames.')

    def mapper_init(self):
        """Set up a logger and initialize counters"""
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                            datefmt='%m-%d %H:%M',
                            filename='./mrtwa.log',
                            filemode='w')
        self.logger = logging.getLogger(__name__)
        self.increment_counter('wa1', 'file_data_bad', 0)
        self.increment_counter('wa1', 'missing_key', 0)
        self.increment_counter('wa1', 'matched', 0)
        self.increment_counter('wa1', 'not_matched', 0)
        
        self.users = set()
        with open(self.options.desired_users) as f:
            for user in f:
                # Add lowercased, stripped users to set
                self.users.add(user.strip().lower())

        if not os.path.exists(self.options.gpg_private):
            self.logger.info('Cannot locate key: {}'.format(
                self.options.gpg_private))
            sys.exit(1)

    def mapper(self, _, line):
        """
        Takes a line specifying a file in an s3 bucket,
        connects to and retrieves all tweets from the file.
        Tweets are _only_ yielded if their user ID belongs to a list of
        high-likelihood-in-West-Africa Twitter users.
        
        :param _: the line number in the file listing the buckets (ignored)
        :param str|unicode line: pseudo-tab separated date, size amd file path
        :return tuple: tweet ID as key, empty body

        """
        cur_line = line.strip()
        aws_prefix, aws_path = cur_line.split('//')

        url = os.path.join('http://s3.amazonaws.com', aws_path)
        try:
            resp = requests.get(url)
        except Exception as e:
            self.increment_counter('resp_exception', type(e).__name__, 1)
            return

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
                try:
                    tweet = entry.feed_entry
                    if tweet.spam_probability > 0.5:
                        self.increment_counter('wa1', 'spam_count', 1)
                        continue

                    # Extract username from the Twitter author URL
                    tweet_identifier = str(tweet.identifier)
                    tweet_link = tweet.link[0].href
                    username = tweet.author[0].link[0].href.split('/')[-1].lower().\
                        decode('utf8').lower()

                    # if username not in self.users:
                    #     self.increment_counter('wa1', 'not_matched', 1)
                    #     continue

                    self.increment_counter('wa1', 'matched', 1)

                    # yield the identifier
                    yield None, ','.join(['ebola_'+tweet_identifier, tweet_link])

                except Exception as e:
                    self.increment_counter('entry_exception', type(e).__name__, 1)

        except Exception as e:
            self.increment_counter('file_exception', type(e).__name__, 1)


if __name__ == '__main__':
    # Set up tries
    # for fname in ['only_west_africa.csv', 'only_other_places.csv', 'CrisisLexRec.csv']:
    #    if not os.path.exists(fname + '.tr'):
    #        write_gazetteer_to_trie_pickle_file(fname)

    # Start Map Reduce Job
    MRUsersToTweets.run()
