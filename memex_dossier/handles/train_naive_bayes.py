'''
Train the Bernoulli NaiveBayes classifier from scikit-learn
to identify usernames.

To run in python,

python train_naive_bayes.py positive-input-data.txt 
       --negative negative-input-data.txt

The --negative command line argument is optional; negative
examples can be created on the fly if it is omitted.

The classifier is saved in naivebayes.pkl.
'''
from __future__ import division, absolute_import

import argparse
import yakonfig
import dblogger

import pickle

import numpy as np
from sklearn.feature_extraction import DictVectorizer
from sklearn.naive_bayes import BernoulliNB

from memex_dossier.handles.create_negative_training_data import create_corpus
from memex_dossier.handles.features import get_all_features, initialize_corpora

def load_data(positive_file, negative_file):
    '''
    loads the data. input args point to the training data files.
    the negative_file can be None.
    '''
    positives = list()

    with open(positive_file, 'r') as f_pos:
        for s in f_pos:
            example = s.strip()
            if not example:
                continue
            positives.append(example)

    ## if there's no negative examples, create them randomly
    ## and with the same length as positive
    if negative_file:
        negatives = list()
        with open(negative_file, 'r') as f_neg:
            for s in f_neg:
                example = s.strip()
                if not example:
                    continue
                negatives.append(example)
    else:
        negatives = create_corpus(len(positives))

    ## initialize corpora
    corpora = initialize_corpora()

    return positives, negatives, corpora

def vectorizer(positives, negatives, corpora):
    '''
    using sklearn.DictVectorizer, transforms input feature vector
    a vector that can be used as input to train the classifier.

    Returns the training matrix X, the labels Y, and the Vectorizer v

    `positives' is the list of positive example usernames
    `negatives' is the list of negative example usernames
    `corpora' is a dictionary of corpora required for generating features

    '''
    v = DictVectorizer(sparse=False)

    D = list()
    y = list()

    for username in positives:
        feat = get_all_features(username, corpora)
        D.append(feat)
        y.append('1')

    for username in negatives:
        feat = get_all_features(username, corpora)
        D.append(feat)
        y.append('0')

    X = v.fit_transform(D)
    Y = np.array(y)

    return X, Y, v


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'positive',
        help='File containing the positive training examples.',
    )
    parser.add_argument(
        '--negative',  
        default=None,
        help='File with negative examples. If omitted, will generate randomly.'
    )
    args = yakonfig.parse_args(parser, [yakonfig, dblogger])
    positive_file = args.positive
    negative_file = args.negative

    positives, negatives, corpora = load_data(positive_file, negative_file)

    ## get features in sklearn format
    X, Y, v = vectorizer(positives, negatives, corpora)

    ## train classifier
    print 'Training classifier.'
    # clf = BernoulliNB(fit_prior=False)
    clf = BernoulliNB()
    clf.fit(X, Y)

    ## save the model and the vectorizer
    output = open('naivebayes.pkl', 'wb' )
    pickle.dump((clf, v) , output)
    output.close()

    print 'Classifier saved in naivebayes.pkl.'

