'''
Create negative training examples for the username / handle classifier.

Random samples from names and english words to create a corpus of fixed size.
'''
from __future__ import division, absolute_import

import argparse
import yakonfig
import dblogger

import random

from nltk.corpus import stopwords, names, words
import names

def create_corpus(N):
    '''
    creates a corpus of negative username training examples of length N
    '''
    english = words.words('en')
    # female_names = names.words('female.txt')
    # male_names = names.words('male.txt')
    examples = set()
    for idx in xrange(N):
        while True:
            which = random.random()
            if which < 0.5:

                word = random.choice(english)

                ## capitalize it a small amount of time
                if random.random() < 0.1:
                    word = word.title()

            elif which <= 1:

                # if random.random() < 0.5:
                #     word = random.choice(female_names)
                # else:
                #     word = random.choice(male_names)

                r = random.random()

                if r < 1/3:
                    word = names.get_full_name()
                elif r < 2/3:
                    word = names.get_first_name()
                else:
                    word = names.get_last_name()

            # ## this makes the naive bayes classifier worse
            # else:
            #     ## phone numbers
            #     n = list()
            #     for idx in xrange(10):
            #         n.append(random.randint(0,9))
            #     num = (n[0], n[1], n[2], n[3], n[4], n[5], n[6], n[7], n[8], n[9])

            #     if random.random() < 0.5:
            #         word = '(%d%d%d) %d%d%d-%d%d%d%d' % num
            #     else:
            #         word = '%d%d%d-%d%d%d-%d%d%d%d' % num

            if not word in examples:
                examples.add(word)
                break

    return list(examples)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'N', 
        help='desired corpus size', 
        type=int
    )
    args = yakonfig.parse_args(parser, [yakonfig, dblogger])
    N = args.N

    examples = create_corpus(N)

    with open('negative-training-data-%d.txt' % N, 'w') as f:
        for word in examples:
            f.write(word + '\n')




 

