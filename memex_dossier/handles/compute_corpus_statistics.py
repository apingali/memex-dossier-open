'''
Compute and store statistics for different nltk corpora
in order to compute quantities like surprisal for a given string.

Defaults to n=2 grams but can be changed using command line arg
'''
from __future__ import division
import os
import sys

import string
import argparse
import yakonfig
import dblogger

from collections import defaultdict
from math import log

from nltk.corpus import stopwords, names, words
from nltk.util import ngrams
from memex_dossier.handles.features import initialize_corpora

def compute_statistics(corpus, n):
    '''
    computes statistics of counts for ngrams in corpus

    `corpus' is a set containing the words in the corpus
    `n' is how many n of ngrams to consider. should be >= 2

    N.B. this assumes that each word is a word and not a string 
         (e.g. with spaces)
    '''

    assert n >= 2

    stats = dict()

    stats['size n'] = n ## i know this is easy to check ...

    ## need both n and n - 1 grams
    for i in xrange(n - 1, n + 1):

        stats[i] = defaultdict(int)

        for word in corpus:

            ## pad the beginning and the end of the word with spaces
            ## this counts beginning of the word characters
            ## this also (to work) assumes that the words have no spaces
            pad = (i - 1)*u' '
            word_padded = pad + word + pad

            for ngram_tuple in ngrams(word_padded, i):
                gram = ''.join(ngram_tuple)
                stats[i][gram] += 1

    ## necessary to compute surprisal  
    ## and this will make it just work
    n_1pad = (n - 2)*u' '        
    stats[n_1pad] = len(corpus) 

    return stats
        


def english_surprise(username, corpora):
    '''
    compute the surprisal of the username using english words as the corpus

    N.B. the corpus stats are in corpora['stats']['english'], which is set up
         like this for a few reasons, including to not break the feature 
         interface
    '''

    ## for now, we sum the surprisal across multiple words
    tokens = username.split()

    stats = corpora['stats']['english']

    s = 0

    for token in tokens:
        s += surprisal(token, stats)

    return s

def male_surprise(username, corpora):
    '''
    compute the surprisal of the username using male names as the corpus

    N.B. the corpus stats are in corpora['stats']['male'], which is set up
         like this for a few reasons, including to not break the feature 
         interface
    '''

    ## for now, we sum the surprisal across multiple words
    tokens = username.split()

    stats = corpora['stats']['male']

    s = 0

    for token in tokens:
        s += surprisal(token, stats)

    return s

def female_surprise(username, corpora):
    '''
    compute the surprisal of the username using female names as the corpus

    N.B. the corpus stats are in corpora['stats']['female'], which is set up
         like this for a few reasons, including to not break the feature 
         interface
    '''

    ## for now, we sum the surprisal across multiple words
    tokens = username.split()

    stats = corpora['stats']['female']

    s = 0

    for token in tokens:
        s += surprisal(token, stats)

    return s


def surprisal(token, stats):
    '''
    given a token and a dictionary of stats, it computes the
    surprisal of the token.

    by default, it will check to find the largest size of
    ngrams computed and use that
    '''
    n = stats['size n']
    corpus_zie = stats['size corpus']

    npad = (n - 1)*u' '
    n_1pad = (n - 2)*u' ' ## i know it's confusing, but it also makes sense

    token_npad = npad + token + npad
    token_n_1pad = npad + token + n_1pad ## this line is correct and not a typo

    ngram_tuples = ngrams(token_npad, n)
    n_1gram_tuples = ngrams(token_n_1pad, n - 1)

    ## these should be the same length
    assert len(ngram_tuples) == len(n_1gram_tuples)

    s = 0

    for idx in xrange(len(ngram_tuples)):

        ng = ''.join(ngram_tuples[idx])
        ncount = stats[n][ng]

        n_1g = ''.join(n_1gram_tuples[idx])
        n_1count = stats[n - 1][n_1g]

        s += log(n_1count, 2) - log(ncount, 2)

    return s



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-n', default=2, type=int,
        help='the n of the ngrams')
    args = yakonfig.parse_args(parser, [yakonfig, dblogger])
    n = args.n

    ## note, these are initialized to be lower case
    corpora = initialize_corpora()

    # which = ['male', 'female', 'english']
    which =['english']

    for corpus_name in which:
        corpus = corpora[corpus_name]
        stats = compute_statistics(corpus, n)

    ## still needs to be saved somewhere

    ## also need to remember how default dict works in order to
    ## hapax legomenoma it

    import pdb; pdb.set_trace()

if __name__ == '__main__':
    main()
