'''
TREC DD 2015
Ebola domain

The goal is to generate ~1M high recall Tweets from West Africa in the last
9 months (from February 2014 to the end of November), during the onset of the
ebola crisis.

This first step identifies the _users_ tweeting from West Africa. Note that this
assumes the user has remained in place for the entire duration.

'''
import mrjob

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol

import datetime
import dateutil
import dateutil.parser
from urllib2 import urlparse
import zlib

# parse code
from twokenize import simpleTokenize
from trie import trie_append, trie_subseq
import cPickle as pickle

# ingest imports
import requests
from streamcorpus import decrypt_and_uncompress
from streamcorpus_pipeline._spinn3r_feed_storage import ProtoStreamReader
import logging
from cStringIO import StringIO

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
    """

    def read(self, line):
        parts = line.split(',', 1)
        if len(parts) == 1:
            parts.append(None)

        return tuple(parts)

    def write(self, key, value):
        ''' Value is expected to be a string already '''
        vals = ','.join((key, value))
        return vals

class MRTwitterWestAfricaUsers(MRJob):
    # Custom parse tab-delimited values
    INPUT_PROTOCOL = RawValueProtocol
    # Serialize messages internally 
    INTERNAL_PROTOCOL = RawValueProtocol
    # Output as csv
    OUTPUT_PROTOCOL = RawCSVProtocol

    
    def mapper_init(self):
        self.logger = logging.getLogger()

    def mapper_get_files(self, line):
        
        parts = line[:-1].split('/')
        url = os.path.join('http://s3.amazonaws.com', *parts[2:])
        resp = requests.get(url)
        
        data = resp.content
        key = 'trec-kba-2013-centralized.gpg-key.private'
        errors, data = decrypt_and_uncompress(data, key)

        if errors:
            self.logger.info('\n'.join(errors))

        f = StringIO(data)
        reader = ProtoStreamReader(f)
        for entry in reader:
            ## entries have other info, see other info here:
            ## https://github.com/trec-kba/streamcorpus-pipeline/blob/master/ 
            ##      streamcorpus_pipeline/_spinn3r_feed_storage.py#L269
            tweet = entry.feed_entry

            time = tweet.last_published
            user_link = tweet.author[0].link[0].href
            user = urlparse.urlsplit(user_link).path[1:]
            body = tweet.title.encode('utf8')

            # Tab-delimited
            yield (None, '{}\t{}\t{}'.format(user, time, body))


    def load_gazetteers(self, filename):
        with open(filename, 'rb') as f:
            return pickle.load(f)
    

    def mapper_getter_init(self):
        self.feb_2014 = datetime.datetime(2014, 2, 1, tzinfo=dateutil.tz.tzutc())
        self.dec_2014 = datetime.datetime(2014, 12, 1, tzinfo=dateutil.tz.tzutc())

        self.utc_7 = datetime.time(7, 0, 0)

        self.west_africa_places = self.load_gazetteers('westAfrica.csv.p')
        self.other_places = self.load_gazetteers('otherPlace.csv.p')

    def steps(self):
        return [
                MRStep(
                    mapper_init = self.mapper_init,
                    mapper = self.mapper_get_files),
                MRStep(
                    mapper_init = self.mapper_getter_init,
                    mapper = self.mapper_get_stats,
                    combiner  = self.combiner_agg_stats,
                    reducer   = self.reducer_agg_stats),

                MRStep(reducer= self.reducer_filter)
                ]
    
    

    def mapper_get_stats(self, _, line):
        """
        Input: stream_corpus feed_entry of single tweet
        Output: 
            (username,
                (
                count,                  # 1
                is_in_west_africa_time, # 0 or 1
                west_africa_loc_mention, # 0 or 1
                other_loc_mention, # 0 or 1
                )
            )

        Discards tweets before February 2014 and after November 2014.
        
        West Africa time is considered to be 0 to +1 UTC.
        7AM to 11PM are taken as daylight hours.

        """
        user, time, tweet= line.split('\t')
        
        # discard tweets out of our window of interest
        t = dateutil.parser.parse(time)
        if t < self.feb_2014 or t > self.dec_2014:
            self.increment_counter('wa1', 'date_invalid', 1)
            return

        self.increment_counter('wa1', 'date_valid', 1)

        ############################################
        # is the tweet in the right time window?
        ############################################
        is_in_time = int(t.time() > self.utc_7)

        ############################################
        # does the tweet contain mention of places?
        ############################################
        
        # tokenize tweet
        tweet_tokens = simpleTokenize(body) 

        # find matches
        west_africa_mention = trie_subseq(tweet_tokens, west_africa_places)
        other_place_mention = trie_subseq(tweet_tokens, other_places)
        
        # return result
        yield user, (1, is_in_time, west_africa_mention, other_place_mention)


    def combiner_agg_stats(self, user, stats):
        yield user, map(sum, zip(*stats))

    def reducer_agg_stats(self, user, stats):
        yield user, map(sum, zip(*stats))

    def reducer_filter(self, user, stats):
        """
        Each stats tuple contains:
            (count, is_in_time, west_africa_mention, other_place_mention)
            for a given Twitter user in the time window.

        Only keep users who 75% of Tweets happen in the right time zone, and that
        mention locations local to West Africa more than elsewhere.

        """

        # More than .75 of the Tweets happen in the right time zone
        # More mentions happen of locations within West Africa than elsewhere
        if stats[1]/float(stats[0]) > 0.75\
                and\
        stats[2] > stats[3]:

            yield user, ','.join(map(str, stats))


if __name__ == '__main__':
    MRTwitterWestAfricaUsers.run()
