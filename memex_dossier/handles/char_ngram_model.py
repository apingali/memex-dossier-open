'''
Determines a weight for a soft-selector string. Strings with lower
(more negative) weights are more rare.
'''
from __future__ import absolute_import, division
import argparse
import gzip
import json
import os

from collections import Counter

from math import log10

def load_ngrams(path=None):
    '''
    Load the unigram and bigram Counter.

    `path` is the path to a gzip json file containing the bigrams
    '''
    if path is None:
        path = os.path.join(os.path.dirname(__file__),
                            'bigrams-cyber1.lower.json.gz')
    i_fh = gzip.open(path)
    bigrams = Counter(dict(json.load(i_fh)))
    unigrams = Counter()

    ## make unigrams
    for k, v in bigrams.iteritems():
        unigrams[k[0]] += v

    return unigrams, bigrams

def bigram_weight(word, unigrams, bigrams):
    '''
    Gets the weight of a word in terms of
    precomputed ngrams. The weight is log p(word).

    Uses a simple markov model over bigrams,
    p(word) = p(w|_)*p(o|w)*p(r|o)*p(d|r)*p(_|d)

    `word` is the soft-selector to be scored.
    `unigrams` is a Counter containing the unigram data
    `bigrams` is a Counter containing the bigram data
    '''
    weight = 0

    ## pad word with leading a trailing spaces and make lower case
    wordpad = ' ' + word.lower() + ' '

    for i in xrange(len(word)+1):
        unigram = wordpad[i:i+1]
        bigram = wordpad[i:i+2]
        if bigrams[bigram] == 0:
            return float('-inf')
        weight += log10(bigrams[bigram]) - log10(unigrams[unigram])

    return weight

def logp_word_length(n, unigrams, bigrams):
    '''
    approximate the probability of words of length `n` using bigram model

    p(length(word) = n) = sum_{c1,c2,..,cn} p(c1c2...cn)

    this returns log p(length(word) = n)

    we assume any character can be in a word other than space
    (i.e. the char ' ' is the only space character. this assumption may
    need to be relaxed in the future)

    Note: this works but is unreasonably slow (obviously due to exponential)
          for n > 2

    `n` is the word length
    `unigrams` is a Counter containing the unigram data
    `bigrams` is a Counter containing the bigram data
    '''

    unigrams_list = unigrams.keys()
    max_score = float('-inf')
    score = 0
    for i in xrange(len(unigrams_list)**n):
        has_space = False ## whether there's a space char or not
        word = '' ## build up work
        idx = i ## index of the jth char
        for j in xrange(n):
            cj = unigrams_list[idx % len(unigrams_list)]
            if cj == ' ':
                has_space = True
            word += cj
            idx = int((idx - (idx % len(unigrams_list)) ) / len(unigrams_list))

        ## skip words with spaces
        if has_space:
            continue

        score_i = bigram_weight(word, unigrams, bigrams)

        ## keep max word for inspection
        if score_i > max_score:
            max_word = word
            max_score = score_i

        score += 10**score_i

    print 'Max word %s with score %f' % (max_word, max_score)
    print 'Overall score: %s' %  log10(score)
    return log10(score)


def main():
    '''
    Get the weight of an input soft selector from the command line.
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('input')
    args = parser.parse_args()
    input_word = args.input

    ## load the ngram dictionary
    unigrams, bigrams = load_ngrams()

    # logp_word_length(3, unigrams, bigrams)

    score = bigram_weight(input_word, unigrams, bigrams)
    print '%s: %f' % (input_word, score)


if __name__ == '__main__':
    main()
