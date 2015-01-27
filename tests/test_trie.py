# -*- coding: utf-8 -*-
__author__ = 'Sam Zhang'

import nose
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from trie import trie_append, _trie_check, trie_subseq

def test_trie_append():
    
    fixtures = (
                # Formatted as a tuple of {before}, {after}, (appended)

                # Test case 1: appending to existing key
                ({ 'Green' : {'Eggs': {'$': None} } }, 
                { 'Green' : {
                        'Eggs': {'$': None} ,
                        'Balls': {'$': None}
                            } },
                ('Green', 'Balls', '$')
                ),

                # Test case 2: appending to new root key
                ({ 'Green' : {'Eggs': {'$': None} } }, 
                { 'Green' : {'Eggs': {'$': None} } ,
                  'Red': {'Eggs': {'$': None}}
                    },
                ('Red', 'Eggs', '$')
                )
            )

    
    for (x, y, a) in fixtures:
        z = trie_append(a, x)
        yield nose.tools.eq_, y, z

def test_trie_check():
    trie = { 'Green' : {
            'Eggs': {'$': None} ,
            'Balls': {'$': None}
            }
        }
    fixtures = (
                # Formatted as a tuple of ( (tokens), present )

                ( ('Green', 'Balls', ), True ),
                ( ('Green', 'Eggs', ), True ),
                ( ('Green', 'Blue', ), False ),
                ( ('Green', ), False ),
                ( (None,), False ),
                ( ('Red', ), False)
                )

    for (x, y) in fixtures:
        z = _trie_check(x, trie)
        yield nose.tools.eq_, z, y

def test_trie_subseq():
    def tokenize(s):
        return s.split(' ') 

    trie = { 'Green' : {
            'Eggs': {'$': None} ,
            'Balls': {'$': None}
            }
        }

    fixtures = (
            ( (tokenize('Good Morning Green Demon'), False),
                (tokenize('Hello Green Balls There'), True),
                (tokenize('Green Balls'), True),
                (tokenize('Green'), False)
            ))
    for (x, y) in fixtures:
        z = trie_subseq(x, trie)

        yield nose.tools.eq_, z, y
    
