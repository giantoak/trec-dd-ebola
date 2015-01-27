# -*- coding: utf-8 -*-
__author__ = 'Sam Zhang'

import nose
from nose import with_setup

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from trie import trie_append, _trie_check, trie_subseq
from twokenize import simpleTokenize
from westafricatwitter import MRTwitterWestAfricaUsers


def load_mapper():
    global m
    m = MRTwitterWestAfricaUsers()
    m.mapper_getter_init()

@with_setup(load_mapper)
def test_mapper_non_empty():
    global m

    # Trivial test that dictionaries aren't empty
    assert len(m.other_places) > 0 and len(m.west_africa_places) > 0

@with_setup(load_mapper)
def test_mapper_subseq_match():
    global m
    
    def test_ss(s):
        if trie_subseq(s, m.west_africa_places):
            return 'wa'
        elif trie_subseq(s, m.other_places):
            return 'other'
        else:
            return None

    # Check to see if locations are found from within Tweets
    fixtures = (
            ('Just went to Ghana', 'wa'),
            ('Paris, the Louvre... I love it #art', 'other'),
            ('No place mentioned here.', None),
            )

    for (x, y) in fixtures:
        t = [x.lower() for x in simpleTokenize(x)]
        z = test_ss(t)

        yield nose.tools.eq_, z, y

