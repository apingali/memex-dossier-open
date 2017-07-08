'''username classifier(s) for determining whether 
   a string is a username or handle

   The classifier is used by instantiating an object
   of type Classifier and passing in a string signifying
   which classifier: e.g.

   nb = Classifier('naivebayes)

   You can then classify a username string by saying

   classification = nb.classify(username)

   which will be a bool. For naivebayes to work,
   you needed to have trained it using 
   train_naive_bayes.py and the file
   naivebayes.pkl needs to exist.

   The classifier will also operate as a streamcorpus transform on
   a StreamItem
'''
from __future__ import division, absolute_import, print_function

import argparse
import yakonfig
import dblogger
import logging

from memex_dossier.fc import StringCounter
import os
import pickle
from streamcorpus import make_stream_item
from streamcorpus_pipeline._tokenizer import nltk_tokenizer
from streamcorpus_pipeline._clean_visible import make_clean_visible
from streamcorpus_pipeline._clean_html import make_clean_html
from memex_dossier.handles.features import get_all_features, initialize_corpora
from memex_dossier.handles.train_naive_bayes import load_data
from memex_dossier.handles.eval_data import load_eval_data, usernames_with_saved_data
import sys


logger = logging.getLogger(__name__)

class Classifier(object):

    def __init__(self, classifier):
        '''
        picks which classifier to use
        '''
        self.classifier_map ={
        'simple':self.classify_simple,
        'naivebayes':self.classify_naive
        }

        assert(set(Classifier.available_classifiers) == \
               set(self.classifier_map.keys())
        )

        self.classifier = classifier
        self.classify = self.classifier_map[self.classifier]

        if self.classifier == 'naivebayes':
            try:
                currrent_dir = os.path.dirname(os.path.abspath(__file__))
                pkl_file = open(currrent_dir + '/naivebayes.pkl', 'r')
                (self.clf, self.v) = pickle.load(pkl_file)
                pkl_file.close()
            except IOError:
                assert 0, 'Pickle file does not exist for NaiveBayes.'


        self.corpora = initialize_corpora()

    ## static list of available classifiers
    available_classifiers = ['simple', 'naivebayes']


    def __call__(self, si, context):
        si = self.process_item(si, context)
        return si

    def process_item(self, si, context=None):
        '''
        runs the classifier on a stream item and stores the classification
        in StreamItem.body.sentences[i]['nltk_tokenizer'].tokens[j].is_username
        '''
        if not si.body or not si.body.sentences:
            return si

        for sentence in si.body.sentences:
            for token in sentences['nltk_tokenizer'].tokens:
                token.is_username = self.classify(token)

        return si

    def build_feature(self, si):
        usernames = StringCounter()
        for sentence in si.body.sentences['nltk_tokenizer']:
            for token in sentence.tokens:
                if len(token.token) <= 3: continue
                if self.classify(token.token.decode('utf8')):
                    usernames[token.token] += 1
        return usernames

    def classify_naive(self, username):
        '''
        Classify `username' using the trained Naive Bayes classifier
        '''
        feat = get_all_features(username, self.corpora)
        return bool(int(self.clf.predict(self.v.fit_transform(feat))[0]))

    def classify_simple(self, username):
        '''
        a simple heuristic for classifying whether input string is a username
        '''
        fv = get_all_features(username, self.corpora)

        ## this assumes the cleanse removes trailing punctuation
        if fv['good_punctuation'] and fv['has letter']:
            return True

        ## have to check for letter so things like phone numbers will fail
        if fv['has number'] and fv['has letter']:
            return True

        ## eliminate names
        if fv['all first capitalized'] and not fv['any not first capitalized']:
            return False

        ## not guaranteed but pretty likely
        if fv['not first capital']:
            return True

        ## prevent phone numbers
        if not(fv['english'] or fv['is male'] or fv['is female']) and fv['has letter']:
            return True

        ## rather than list the ways it is not, can just return false here
        ## and for performance, could turn off other features.
        return False

        # ## self explanatory
        # if fv['is_stop_word']:
        #     return False


def extract_user_names(clean_visible):
    '''also renamedto usernames2'''

    if isinstance(clean_visible, unicode):
        clean_visible = clean_visible.encode('utf8')

    si = make_stream_item(0, '')
    si.body.clean_visible = clean_visible
    
    xform = nltk_tokenizer({})
    xform.process_item(si)

    classifier = Classifier('naivebayes')
    sc = classifier.build_feature(si)

    logger.info('found usernames: %r', sc)
    open('/tmp/found.txt', 'wb').write('\n'.join(map(lambda x: x.encode('utf8'), sc.keys())))

    return sc


def score(is_usernames, labels):
    '''
    Compare the classification output in `is_usernames' to the ground truth
    data in `labels'. Compute precision, recall, and f-score.

    `is_usernames' is a list of classifications
    `labels' is a list of labels

    (obviously the indices should align)
    '''
    assert len(is_usernames) == len(labels)

    TP = 0
    FN = 0
    TN = 0
    FP = 0

    for i in xrange(len(is_usernames)):
        is_username = is_usernames[i]
        label = labels[i]
        TP += is_username and label
        TN += not is_username and not label
        FP += is_username and not label
        FN += not is_username and label

    P = TP / (TP + FP)
    R = TP / (TP + FN)
    F = 2*P*R / (P + R)

    return {'P': P, 'R': R, 'F': F}

if __name__== '__main__':
    '''
    scores all available classifiers on a test set
    '''
    # username = '434-432-1232'
    # simple = Classifier('simple')
    # nb = Classifier('naivebayes')

    # print 'is a (simply) a username? %r' % simple.classify(username)
    # print 'is a (nb) a username? %r' % nb.classify(username)

    parser = argparse.ArgumentParser()
    parser.add_argument('positive', help='File containing the positive test examples.')
    parser.add_argument('--negative', default=None, help='File with negative examples. If omitted, will generate randomly.')
    parser.add_argument('--test-text', 
                        help=('name of entity for whom data has been saved: %r' % 
                              usernames_with_saved_data))
    args = yakonfig.parse_args(parser, [yakonfig, dblogger])

    if args.test_text:
        for eg, tr in load_eval_data(args.test_text):
            eg = make_clean_html(eg)
            eg = make_clean_visible(eg)
            sc = extract_user_names(eg)
            found = set(sc)
            expected = set(tr)
            TP = found.intersection(expected)
            FN = expected - found
            FP = found - expected
            print('TP: \n\t%s' % '\n\t'.join(TP))
            print('\n\nFN: \n\t%s' % '\n\t'.join(FN))
            print('\n\nFP: \n\t%s' % '\n\t'.join(FP))
            P = len(TP) / (len(TP) + len(FP))
            R = len(TP) / (len(TP) + len(FN))
            F = 2 * P * R / (P + R)
            print('F=%.4f, P=%.4f, R=%.4f, TP=%d, FN=%d, FP=%d' % (F, P, R, len(TP), len(FN), len(FP)))
            #print sc
        sys.exit()

    positive_file = args.positive
    negative_file = args.negative

    print('loading data')

    positives, negatives, corpora = load_data(positive_file, negative_file)

    ## load labels
    labels = list()
    for username in positives:
        labels.append(True)
    for username in negatives:
        labels.append(False)

    print('classifying and scoring')

    ## classify using all the classifiers
    for classifier_name in Classifier.available_classifiers:

        classifications = list()
        classifier = Classifier(classifier_name)

        for username in positives:
            is_username = classifier.classify(username)
            classifications.append(is_username)

        for username in negatives:
            is_username = classifier.classify(username)
            classifications.append(is_username)

        ## score training set, compute f-scores
        scores = score(classifications, labels)
        print('Classifier: %s, Precision: %f, Recall: %f, F-score: %f' % \
              (classifier.classifier, scores['P'], scores['R'], scores['F']))


