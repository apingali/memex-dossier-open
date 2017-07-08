'''Tests for memex_dossier.web.filter_preds filtering functions

.. This software is released under an MIT/X11 open source license.
   Copyright 2015 Diffeo, Inc.
'''
import copy
from itertools import chain, repeat
import pytest
import random
import string
import time

from memex_dossier.fc import FeatureCollection as FC
from memex_dossier.fc import FeatureCollection, StringCounter, GeoCoords
from nilsimsa import Nilsimsa

from memex_dossier.web.tests import config_local, kvl, label_store  # noqa
from memex_dossier.web.filters import nilsimsa_near_duplicates, geotime


def nilsimsa_hash(text):
    if isinstance(text, unicode):
        text = text.encode('utf8')
    return Nilsimsa(text).hexdigest()


near_duplicate_texts = [
    'The quick brown fox jumps over the lazy dog.',
    'The quick brown fox jumps over the lazy dogs.',
    'The quick brown foxes jumped over the lazy dog.',
    'The quick brown foxes jumped over the lazy dogs.',
]


def make_fc(text):
    nhash = nilsimsa_hash(text)
    fc = FeatureCollection()
    fc['#nilsimsa_all'] = StringCounter([nhash])
    return fc


candidate_chars = (
    string.ascii_lowercase + string.ascii_uppercase + string.digits
)
# make whitespaces appear approx 1/7 times
candidate_chars += ' ' * (len(candidate_chars) / 7)


def random_text(N=3500):
    '''generate a random text of length N
    '''
    return ''.join(random.choice(candidate_chars) for _ in range(N))


def mutate(text, N=1):
    '''randomly change N characters in text
    '''
    new_text = []
    prev = 0
    for idx in sorted(random.sample(range(len(text)), N)):
        new_text.append(text[prev:idx])
        new_text.append(random.choice(candidate_chars))
        prev = idx + 1
    new_text.append(text[prev:])
    return ''.join(new_text)


@pytest.mark.skipif('1')  # no need to run this
@pytest.mark.xfail
def test_nilsimsa_exact_match():
    '''check that even though Nilsimsa has 256 bits to play with, you can
    pretty easily discover non-idential texts that have identical
    nilsimsa hashes.

    '''
    text0 = random_text(10**5)
    for _ in range(100):
        text1 = mutate(text0, N=1)
        if text0 != text1:
            assert nilsimsa_hash(text0) != nilsimsa_hash(text1)



def test_geotime_filter():
    fname = '!both_co_LOC_1'
    gc1 = GeoCoords({'foo': [(10, 10, 10, None)]})
    gc2 = GeoCoords({'foo': [(10, 10, 10, None), (-10, 10, 10, 10)]})
    gc3 = GeoCoords({'foo': [(-10, 10, 10, None), (10, 10, 10, 10)]})

    fc1 = FC()
    fc1[fname] = gc1
    fc2 = FC()
    fc2[fname] = gc2
    fc3 = FC()
    fc3[fname] = gc3

    pred = geotime().set_query_params({
        'min_lat': 0, 'max_lat': 20,
        'min_lon': -20, 'max_lon': 0,
        'min_time': 0,
    }).create_predicate()

    results = filter(pred, [('', fc1), ('', fc2), ('', fc3)])
    assert len(results) == 1
    assert results[0][1] == fc2
