# -*- coding: utf-8 -*-

"""
Copyright (c) 2015 by Diffeo

The function to score a username is 
score_string(query_string, char_unigrams, char_bigrams)

Based on Python Word Segmentation found here:
https://github.com/grantjenks/wordsegment

Next Copyright (c) 2015 by Grant Jenks

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

English Word Segmentation in Python

Word segmentation is the process of dividing a phrase without spaces back
into its constituent parts. For example, consider a phrase like "thisisatest".
For humans, it's relatively easy to parse. This module makes it easy for
machines too. Use `segment` to parse a phrase into its parts:

>>> from wordsegment import segment
>>> segment('thisisatest')
['this', 'is', 'a', 'test']

In the code, 1024908267229 is the total number of words in the corpus. A
subset of this corpus is found in unigrams.txt and bigrams.txt which
should accompany this file. A copy of these files may be found at
http://norvig.com/ngrams/ under the names count_1w.txt and count_2w.txt
respectively.

Copyright (c) 2015 by Grant Jenks

Based on code from the chapter "Natural Language Corpus Data"
from the book "Beautiful Data" (Segaran and Hammerbacher, 2009)
http://oreilly.com/catalog/9780596157111/

Original Copyright (c) 2008-2009 by Peter Norvig
"""
from __future__ import absolute_import, division
from collections import Counter
import sys
import os
from os.path import join, dirname, realpath
from math import log10, sqrt
from functools import wraps
import many_stop_words
import argparse
import regex as re
import string
import wordsegment as ws
from memex_dossier.handles.char_ngram_model import bigram_weight, load_ngrams
import logging
logger = logging.getLogger(__name__)

ALPHABET = set('abcdefghijklmnopqrstuvwxyz0123456789')

if sys.hexversion < 0x03000000:
    range = xrange

# def parse_file(filename):
#     "Read `filename` and parse tab-separated file of (word, count) pairs."
#     with open(filename) as fptr:
#         lines = (line.split('\t') for line in fptr)
#         return dict((word, float(number)) for word, number in lines)

# basepath = join(dirname(realpath(__file__)), 'wordsegment_data')
# unigram_counts = parse_file(join(basepath, 'unigrams.txt'))
# bigram_counts = parse_file(join(basepath, 'bigrams.txt'))

def divide(text, limit=24):
    """
    Yield `(prefix, suffix)` pairs from `text` with `len(prefix)` not
    exceeding `limit`.
    """
    for pos in range(1, min(len(text), limit) + 1):
        yield (text[:pos], text[pos:])

TOTAL = 1024908267229.0

def score(word, prev=None, char_unigrams=None, char_bigrams=None):
    "Score a `word` in the context of the previous word, `prev`."

    if prev is None:
        if word in ws.unigram_counts:

            # Probability of the given word.

            return ws.unigram_counts[word] / TOTAL
        else:
            # Penalize words not found in the unigrams according
            # to their length, a crucial heuristic.

            if char_unigrams is None:
                return 10.0 / (TOTAL * 10 ** len(word))
            else:
                logweight = bigram_weight(word, char_unigrams, char_bigrams)
                return 10**logweight

    else:
        bigram = '{0} {1}'.format(prev, word)

        if bigram in ws.bigram_counts and prev in ws.unigram_counts:

            # Conditional probability of the word given the previous
            # word. The technical name is *stupid backoff* and it's
            # not a probability distribution but it works well in
            # practice.
            prev_score = score(prev,
                                char_unigrams=char_unigrams,
                                char_bigrams=char_bigrams
            )
            return ws.bigram_counts[bigram] / TOTAL / prev_score
        else:
            # Fall back to using the unigram probability.

            return score(word,
                            char_unigrams=char_unigrams,
                            char_bigrams=char_bigrams
            )

def clean(text):
    "Return `text` lower-cased with non-alphanumeric characters removed."
    return ''.join(letter for letter in text.lower() if letter in ALPHABET)

def segment(text, char_unigrams=None, char_bigrams=None):
    "Return a list of words that is the best segmenation of `text`."

    memo = dict()

    def search(text, prev='<s>'):
        if text == '':
            return 0.0, []

        def candidates():
            for prefix, suffix in divide(text):
                pscore = score(prefix,
                        prev=prev,
                        char_unigrams=char_unigrams,
                        char_bigrams=char_bigrams
                )
                if pscore > 0:
                    prefix_score = log10(pscore)
                else:
                    prefix_score = float('-inf')
                pair = (suffix, prefix)
                if pair not in memo:
                    memo[pair] = search(suffix, prefix)
                suffix_score, suffix_words = memo[pair]

                yield (prefix_score + suffix_score, [prefix] + suffix_words)

        return max(candidates())

    try:
        result_score, result_words = search(clean(text))
        return result_words, result_score
    except Exception as exc:
        logger.warn('bang! on %r --> %s', text, exc)
        return text, 0.0


def p_of_word(n):
    '''
    computes the probability of a word of length `n`
    using the word unigrams

    NOTE: this should probably be redone using bigrams

    returns log10 of that probability.

    Note: fails for n > 24

    `n` is the length of the word
    '''

    if n > 24:
        return float('-inf')

    count_n = 0
    total_count =0
    for k,v in ws.unigram_counts.iteritems():
        total_count += v
        if len(k) == n:
            count_n += v

    return log10(count_n) - log10(total_count)

def get_norm(n, char_unigrams, char_bigrams):
    '''
    really hacky normalization for surprisal
    '''
    import random
    def weighted_choice(choices):
        total = sum(w for c, w in choices.iteritems())
        r = random.uniform(0, total)
        upto = 0
        for c, w in choices.iteritems():
          if upto + w >= r:
             return c
          upto += w
        assert False, "Shouldn't get here"


    norm = 0
    N = int(200/n) + 1

    for j in xrange(N):
        randstring = ''
        for i in xrange(n):
            c =  weighted_choice(char_unigrams)
            while c == ' ':
                c =  weighted_choice(char_unigrams)
            randstring += c

    # print randstring
    # print bigram_weight(randstring, char_unigrams, char_bigrams)
    # print segment(randstring, char_unigrams, char_bigrams)
        s_j = bigram_weight(randstring, char_unigrams, char_bigrams)
        while s_j == float('-inf'):
            randstring = ''
            for i in xrange(n):
                c =  weighted_choice(char_unigrams)
                while c == ' ':
                    c =  weighted_choice(char_unigrams)
                randstring += c
            s_j = bigram_weight(randstring, char_unigrams, char_bigrams)

        norm += s_j/N

    return norm

enable_path = os.path.join(os.path.dirname(__file__), 'enable1.txt')
enable = set(open(enable_path).read().splitlines())
countries_path = os.path.join(os.path.dirname(__file__), 'countries.txt')
countries = set(map(lambda x: x.split('|')[1].lower(),
                     open(countries_path).read().splitlines()))
enable.update(many_stop_words.get_stop_words())
enable.update(countries)

def reject(query_string, char_unigrams, char_bigrams):
    if len(query_string) < 4 or len(query_string) > 15 or \
           query_string in enable or query_string.lower() in enable \
           or any((string in query_string
                   for string in ['Shopping', 'Account', 'Checkout'])):
        # not words
        return True
    else:
        return False

def score_string(query_string, char_unigrams, char_bigrams):
    '''
    This is the function that is actually used to score a username.
    '''
    if reject(query_string, char_unigrams, char_bigrams):
        return 0.0
    words, score = segment(query_string, char_unigrams, char_bigrams)
    if len(words) >= 3 and all(word in enable for word in words):
        return 0.9 # ah, yes.  The Rule.
    norm = get_norm(len(query_string), char_unigrams, char_bigrams)
    return min(1, score / norm)**(10/len(query_string))

digits = set(string.digits)
letters = set(string.letters)

def prob_username(query_string, char_unigrams, char_bigrams):
    '''
    This is the function that is actually used to score a username.
    '''
    if reject(query_string, char_unigrams, char_bigrams):
        return 0.0

    if len(query_string) > 30:
        return 1.0

    words, score = segment(query_string, char_unigrams, char_bigrams)
    if len(words) >= 3 and all(word in enable for word in words):
        return 0.9 # ah, yes.  The Rule.

    qset = set(query_string)
    if qset.intersection(digits) and qset.intersection(letters) \
           and len((qset - letters) - digits) == 0:
        return 0.7

    if query_string == ''.join([word.capitalize() for word in words]):
        return 0.8

    norm = get_norm(len(query_string), char_unigrams, char_bigrams)
    return min(0.65, min(1, score / norm)**(10/len(query_string)))


def dot(c1, c2):
    if len(c1) < len(c2):
        shorter = c1
        longer = c2
    else:
        shorter = c2
        longer = c1

    return sum(longer.get(k, 0) * v for k, v in shorter.iteritems())


def cosine(v1, v2):
    if not (v1 and v2):
        return 0.0
    dot_prod = dot(v1, v2)
    norm_sq1 = dot(v1, v1)
    norm_sq2 = dot(v2, v2)

    norms = sqrt(norm_sq1 * norm_sq2)
    if norms == 0:
        return 0.
    else:
        return dot_prod / norms


def _username_comparison(usernameA, usernameB, char_unigrams, char_bigrams):
    '''used by make_username_comparison to compare the similarity of two usernames
    '''
    if usernameA == usernameB:
        return 1.0

    wordsA, scoreA = segment(usernameA, char_unigrams, char_bigrams)
    wordsB, scoreB = segment(usernameB, char_unigrams, char_bigrams)
    vecA = Counter(map(clean, wordsA))
    vecB = Counter(map(clean, wordsB))
    return cosine(vecA, vecB)


def make_username_comparison():
    char_unigrams, char_bigrams = load_ngrams()
    def _closure(usernameA, usernameB):
        return _username_comparison(usernameA, usernameB, char_unigrams, char_bigrams)
    return _closure

username_comparison = make_username_comparison()


def main():
    '''
    Get the weight of an input soft selector from the command line.

    Then print various versions of a score for testing
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('input')
    parser.add_argument('--compare')
    parser.add_argument('--ngrams')
    args = parser.parse_args()
    input_string = args.input

    if args.compare:
        print username_comparison(args.input, args.compare)
        sys.exit()

    ## load the char_ngram dictionaries
    char_unigrams, char_bigrams = load_ngrams(args.ngrams)

    # score just from bigrams
    score1 = bigram_weight(input_string, char_unigrams, char_bigrams)

    # score from word segmenter
    words, score2 = segment(input_string)

    # score from word segmenter but char model for words it doesn't know
    words, score3 = segment(input_string, char_unigrams, char_bigrams)

    print '%s, by char: %f' % (input_string, score1)
    print 'segmented as %r: %f' % (words, score2)
    print 'with char for words seg does not know: %f' % score3




    typical_per_char = log10(len(char_unigrams.keys()))
    n = len(input_string)

    print 'as probability'

    print '%s, by char: %f' % (input_string, -score1 / (typical_per_char*n))
    print 'segmented as %r: %f' % (words, -score2 / (typical_per_char*n))
    print 'with char for words seg does not know: %f' % (-score3/ (typical_per_char*n))



    norm = get_norm(n, char_unigrams, char_bigrams)
    print norm

    print
    print 'better as probability'
    #
    print '%s, by char: %f' % (input_string, score1 / norm)
    print 'with char for words seg does not know: %f' % \
            score_string(input_string, char_unigrams, char_bigrams)
    print 'prob_username: %f' % prob_username(input_string, char_unigrams, char_bigrams)

if __name__ == '__main__':
    main()

    ## code used to process file of names for demo
