'''
Features for username classification.


                if it[0] in rejects: continue
                if it[0] in stops: continue
                if it[0] not in keepers: continue
                if '&' in it[0]: continue
                if '=' in it[0]: continue
                if ':' in it[0]: continue
                if ' ' in it[0]: continue
                if len(it[0]) < 4: continue
                if len(it[0]) > 10: continue
                is_username = userclf.classify(it[0])
                logger.info('%r is_username=%r', it[0], is_username)

'''
from __future__ import division, absolute_import
import os
import sys
import regex as re
import string
import argparse
import yakonfig
import dblogger

from nltk.corpus import stopwords, names, words


allowed_punctuation = set('._-')
def has_username_allowed_punctuation(username, corpora):
    '''
    Is there allowed punctuation in the candidiate `username`

    '''
    pset = allowed_punctuation
    for c in username:
        if c in pset:
            return 1
    return 0

def has_username_disallowed_punctuation(username, corpora):
    '''
    Is there disallowed punctuation in the candidiate `username`

    '''
    pset = set(string.punctuation)
    pset.difference(allowed_punctuation)
    for c in username:
        if c in pset:
            return 1
    return 0

allowed_format_re = re.compile(ur'^\w(?:\w*(?:[.-_]\w+)?)*(?<=^.{4,32})$')
def matches_allowed_format(username, corpora):
    return bool(allowed_format_re.match(username))

def has_unicode(username, corpora):
    '''
    is there any unicode in the word

    '''
    if not isinstance(username, unicode):
        username = username.decode('utf8')
    for c in username:
        if ord(c) > 128: return 1
    return 0

def is_stop_word(username, corpora):
    '''
    Is it a stop word?
    '''
    if username in corpora['stop'] :
        return 1
    else:
        return 0

def first_letter_capital(username, corpora):
    '''
    Is the first letter capitalized?
    '''
    if username[0].isupper():
        return 1
    else:
        return 0

def not_first_letter_capital(username, corpora):
    '''
    Is any letter other than the first letter capitalized?
    '''
    for idx in xrange(1, len(username)):
        if username[idx].isupper():
            return 1
    return 0

def has_number(username, corpora):
    '''
    Is there a number in the word?
    '''
    for c in username:
        if c in set(string.digits):
            return 1
    return 0

def has_letter(username, corpora):
    '''
    Is there a letter in the word?
    '''
    for c in username:
        if c in set(string.ascii_letters):
            return 1
    return 0

def is_male_name(username, corpora):
    '''
    Is it a male name?
    '''
    if username.lower() in corpora['male'] :
        return 1
    return 0

def is_female_name(username, corpora):
    '''
    Is it a female name?
    '''
    if username.lower() in corpora['female'] :
        return 1
    return 0

def is_english(username, corpora):
    '''
    Is it an english word?
    '''
    if username.lower() in corpora['english']:
        return 1
    return 0

LEET_MAP = {
    u'1': u'i',
    u'3': u'e',
    u'7': u't',
    u'0': u'o',
    }

def leet(s):
    if not isinstance(s, unicode):
        s = s.decode('utf8')
    leet_map = dict([(ord(f), ord(t)) for f, t in LEET_MAP.items()])
    candidates = [s.translate(leet_map)]
    leet_map[ord(u'1')] = ord(u'l')
    candidates.append(s.translate(leet_map))
    return candidates

def is_leet_speak(username, corpora):
    '''
    Does the 1337 transform map it to an English word?
    '''
    return all(any(c.lower() in corpora['english'] for c in leet(part))
               for part in username.split())

def string_len(username, corpora):
    '''
    How long is the username?
    '''
    return len(username)

def tokens(username, corpora):
    '''
    How many tokens?
    '''
    return len(username.split())

def first_letter_capital_each_token(username, corpora):
    '''
    Is the first letter capitalized for each token?
    '''
    tokens = username.split()
    for token in tokens:
        if not token[0].isupper():
            return 0
    return 1

def any_not_first_capital(username, corpora):
    '''
    Is any letter capitalized that's not the first, for each token?
    '''
    tokens = username.split()
    for token in tokens:
        for idx in xrange(1, len(token)):
            if token[idx].isupper():
                return 1
    return 0

available_features = {
    'is_leet_speak': is_leet_speak,
    'good_punctuation': has_username_allowed_punctuation,
    'bad_punctuation': has_username_disallowed_punctuation,
    'format': matches_allowed_format,
    'has_unicode': has_unicode,
    'stop word': is_stop_word,
    'first capitalized': first_letter_capital,
    'not first capital': not_first_letter_capital,
    'has number': has_number,
    'has letter': has_letter,
    'is male': is_male_name,
    'is female': is_female_name,
    'english': is_english,
    'length': string_len,
    'tokens': tokens,
    'all first capitalized': first_letter_capital_each_token,
    'any not first capitalized': any_not_first_capital
    }

def initialize_corpora():
    '''
    returns a dictionary of the different corpora
    '''
    corpora = dict()
    corpora['english'] = set(map(unicode.lower, words.words('en')))
    corpora['male'] = set(map(unicode.lower, names.words('male.txt')))
    corpora['female'] = set(map(unicode.lower, names.words('female.txt')))
    corpora['stop'] = set(stopwords.words('english'))

    return corpora

def get_all_features(username, corpora):
    '''
    Takes an input username and makes a dictionary of all features
    in `available_features'
    '''
    fv = dict()
    for feature_name, feature in available_features.iteritems():
        fv[feature_name] = feature(username, corpora)
    return fv

# def convert_fv_to_string(fv, precision=1000):
#     '''
#     Takes a feature vector `fv' and converts it to a string
#     representation of the features as required for seqlearn
#     and the sklearn.feature_extraction.FeatureHasher (the 'hashing trick')

#     `precision' stores how many digits the floats are rounded to in storing
#     the features
#     '''
#     for feature, val in fv.iteritems():
#         rounded_val = round(val*precision) ## nearest int between 0 and 100
#         yield '%s:%d' % (feature, rounded_val)





if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'username', 
        help='an example username')
    args = yakonfig.parse_args(parser, [yakonfig, dblogger])
    username = args.username

    corpora = initialize_corpora()

    feat = get_all_features(username, corpora)
    print 'username: %s\nfeatures: %r' % (username, feat)


    # print int(simple_classifier(username, corpora))
    # sys.stdout.flush()
    




