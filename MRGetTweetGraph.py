# -*- coding: utf-8 -*-
__author__ = 'Sam Zhang, Peter M. Landwehr'
"""
Read in tweets. If the user or the mentioned users are in a predefined list,
extract the edge.
Return edges if they are over a given threshold in both directions
Edges are yielded as soon as they pass the threshold, so all weights are minimums.
"""
import codecs
import logging
import os
from cStringIO import StringIO

import dateutil
import dateutil.parser
from mrjob.job import MRJob
from mrjob.protocol import PickleProtocol
from mrjob.protocol import RawValueProtocol
import requests
import sys

# parse code
from twokenize import simpleTokenize
import marisa_trie

# ingest imports
from streamcorpus import decrypt_and_uncompress
from streamcorpus_pipeline._spinn3r_feed_storage import ProtoStreamReader


MIN_BIDIRECTIONAL_WEIGHT = 2


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
    :param marisa_trie.Trie trie: trie of words
    :return bool: True if some subset of the tweet's words are in the trie, else False
    """
    cur_uni = tweet_tokens[-1]
    if trie.has_keys_with_prefix(cur_uni):
        return True
    for i in xrange(len(tweet_tokens) - 2, -1, -1):
        cur_uni = tweet_tokens[i] + ' ' + cur_uni
        if trie.has_keys_with_prefix(cur_uni):
            return True
    return False


def get_edge_key_value_pair(source_uni, sink_uni):
    if source_uni < sink_uni:
        return source_uni.encode('utf8')+'@'+sink_uni.encode('utf8'), (1, 0)
    return sink_uni.encode('utf8')+'@'+source_uni.encode('utf8'), (0, 1)


class MRGetTweetGraph(MRJob):
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
        super(MRGetTweetGraph, self).configure_options()
        self.add_file_option('--gpg-private',
                             default='trec-kba-2013-centralized.gpg-key.private',
                             help='path to gpg private key for decrypting the data')
        self.add_file_option('--desired-users',
                             default='usernames.csv.tr',
                             help='path to pickled trie of desired usernames')

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
        self.increment_counter('wa1', 'file_date_exception', 0)
        self.increment_counter('wa1', 'file_data_bad', 0)
        self.increment_counter('wa1', 'tweet_date_valid', 0)
        self.increment_counter('wa1', 'tweet_date_invalid', 0)
        self.increment_counter('wa1', 'tweet_date_exception', 0)
        self.increment_counter('wa1', 'spam_count', 0)
        self.increment_counter('wa1', 'no_mentions', 0)
        self.increment_counter('wa1', 'has_mentions', 0)
        self.increment_counter('wa1', 'known_user', 0)
        self.increment_counter('wa1', 'known_mention', 0)
        self.increment_counter('wa1', 'edge_finding_exception', 0)

        if not os.path.exists(self.options.gpg_private):
            self.logger.info('Cannot locate key: {}'.format(
                self.options.gpg_private))
            sys.exit(1)

        self.naive_feb_2014 = dateutil.parser.parse('2014-02-01')
        self.naive_dec_2014 = dateutil.parser.parse('2014-12-01')
        self.feb_2014 = dateutil.parser.parse('2014-02-01 00:00:00+00:00')
        self.dec_2014 = dateutil.parser.parse('2014-12-01 00:00:00+00:00')

        self.username_trie = load_trie_from_pickle_file(self.options.desired_users)

    def mapper(self, _, line):
        """
        Takes a line specifying a file in an s3 bucket,
        connects to and retrieves all tweets from the file.
        If the user is known, yields edges for all  mentioned users.
        Alterantely, if a mentioned user is known, yield an edge
        Edges are _only_ yielded if tweets fall within the target date range
        (February 1, 2014 - November 30, 2014)
        :param _: the line number in the file listing the buckets (ignored)
        :param str|unicode line: pseudo-tab separated date, size amd file path
        :return tuple: user, mentioned user
        """
        aws_prefix, aws_path = line.strip().split('//')
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
            self.logger('{}: did not retrieve any data. Skipping...\n'.format(aws_path))
            self.increment_counter('wa1', 'file_data_bad', 1)
            return

        if not os.path.exists(self.options.gpg_private):
            self.logger.info('Cannot locate key: {}'.format(
                self.options.gpg_private))
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

            mentions_uni = [tok[1:].lower() for tok in simpleTokenize(tweet.title) if tok[0] == '@']
            if len(mentions_uni) == 0:
                self.increment_counter('wa1', 'no_mentions', 1)
                continue

            self.increment_counter('wa1', 'has_mentions', 1)

            try:
                # user_scrn_uni = tweet.author[0].name.split(' (')[0]
                user_scrn_uni = tweet.author[0].link[0].href.split('/')[-1].lower().decode('utf8')
                if user_scrn_uni in self.username_trie:
                    # self.increment_counter('u_'+user_scrn_uni, tweet.title, 1)
                    # self.increment_counter('wa1', 'known_user', len(mentions_uni))
                    for mention in mentions_uni:
                        if mention not in self.username_trie:
                            yield get_edge_key_value_pair(user_scrn_uni, mention)
                else:
                    for mention in mentions_uni:
                        if mention in self.username_trie:
                            # self.increment_counter('m_'+mention+'@u_'+user_scrn_uni, tweet.title, 1)
                            # self.increment_counter('wa1', 'known_mention', 1)
                            yield get_edge_key_value_pair(user_scrn_uni, mention)
            except:
                self.increment_counter('wa1', 'edge_finding_exception', 1)

    def combiner(self, edge_name, edge_weight_tuples):
        """
        :param str edge_name: Alpha-sorted name of the edge
        :param list edge_weight_tuples: list (generator) of 0,1 and 1,0 edge weights
        :return tuple: edge_name, combined edge weights
        """
        cur_in = 0
        cur_out = 0
        for tpl in edge_weight_tuples:
            cur_in += tpl[0]
            cur_out += tpl[1]
            if cur_in >= MIN_BIDIRECTIONAL_WEIGHT and cur_out >= MIN_BIDIRECTIONAL_WEIGHT:
                yield edge_name, (cur_in, cur_out)

        yield edge_name, (cur_in, cur_out)

    def reducer(self, edge_name, edge_weight_tuples):
        """
        Get all edges from each user. We want to specifically keep all users
        who mentioned each other at least three times. Edges less than three can
        be dropped.
        :param str|unicode edge_name: The particular edge
        :param list edge_weight_tuples: list (generator) of direction weights
        :return tuple: None, tab-separated str of edge name and directional weights
        """

        cur_in = 0
        cur_out = 0
        for tpl in edge_weight_tuples:
            cur_in += tpl[0]
            cur_out += tpl[1]

            if cur_in >= MIN_BIDIRECTIONAL_WEIGHT and cur_out >= MIN_BIDIRECTIONAL_WEIGHT:
                yield None, '\t'.join([edge_name, str(cur_in), str(cur_out)])


if __name__ == '__main__':
    MRGetTweetGraph.run()
