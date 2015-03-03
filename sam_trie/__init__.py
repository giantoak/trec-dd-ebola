# -*- coding: utf-8 -*-
__author__ = 'Sam Zhang'
"""
Simple prefix tree class for fast lookup of n-grams
"""

# TODO: refactor trie code into a class...
# TODO: accept tokenizer as argument, add ending delimiter internally

import cPickle as pickle


def write_gazetteer_to_trie_pickle_file(filename):
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


def load_trie_from_pickle_file(filename):
    with open(filename, 'rb') as f:
        return pickle.load(f)


def trie_append(parts, trie):
    """
    Recursively append to a trie of tokens (prefix tree) from a list of tokens.
    Destructive.
    Assumes tokens are at least len 2.
    """
    
    x, rest = parts[0], parts[1:]
    
    # base case: construct leaf node
    if len(rest) == 1:
        rest = rest[0]
        trie[x] = {rest: None}
        return trie
    
    try:
        # append to existing branch
        trie[x] = trie_append(rest, trie[x])
    except KeyError:
        # create new branch
        trie[x] = trie_append(rest, {})

    return trie


def _trie_check(tokens, trie, end_delim='$'):
    """
    Check whether a list of tokens is present in trie.
    Greedily quits when an end delimiter is found.
    Assumes tokens is at least len 2.
    """
    
    # End delimiter is found;
    if end_delim in trie:
        return True
    
    try:
        x, rest = tokens[0], tokens[1:]
    except IndexError:
        # No end delimiter found, and no more tokens are left
        return False

    try:
        return _trie_check(rest, trie[x])
    except KeyError:
        return False


def trie_subseq(seq, trie):
    """
    Checks for any matching subsequence in seq in trie.
    """

    for i in range(len(seq)):
        if _trie_check(seq[i:], trie):
            return True
    
    return False
