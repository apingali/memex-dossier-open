'''memex_dossier.fc Feature Collections

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

'''
from __future__ import absolute_import, division, print_function
try:
    from collections import Counter
except ImportError:
    from backport_collections import Counter

import pytest

from memex_dossier.fc import FeatureCollection, StringCounter, ReadOnlyException
from memex_dossier.fc.tests import counter_type


def test_read_only():
    fcwork = FeatureCollection({'feat': {'foo': 1}})

    fc = FeatureCollection()
    fc['feat']['foo'] += 1
    fc.read_only = True
    with pytest.raises(ReadOnlyException):
        fc += fcwork
    with pytest.raises(ReadOnlyException):
        fc -= fcwork
    with pytest.raises(ReadOnlyException):
        fc -= fcwork
    with pytest.raises(ReadOnlyException):
        del fc['feat']
    with pytest.raises(ReadOnlyException):
        fc.pop('feat')


def test_read_only_binop():
    fc1 = FeatureCollection({'NAME': {'foo': 1, 'bar': 1}})
    fc2 = FeatureCollection({'NAME': {'foo': 2, 'bar': 2}})

    fc1.read_only = True
    fc2.read_only = True

    result = fc1 + fc2
    expected = FeatureCollection({'NAME': {'foo': 3, 'bar': 3}})
    assert result == expected
    assert not result.read_only


def test_read_only_features():
    fc = FeatureCollection({'feat': StringCounter({'foo': 1})})
    fc['feat']['foo'] += 1
    fc.read_only = True

    with pytest.raises(ReadOnlyException):
        fc['feat']['foo'] += 1
    with pytest.raises(ReadOnlyException):
        fc['feat'].pop('foo')
    with pytest.raises(ReadOnlyException):
        del fc['feat']['foo']


def test_identity():
    fc = FeatureCollection()
    fc.read_only = True
    id(fc['one']) == id(fc['two'])


def test_read_only_preserved_after_serialized():
    fc = FeatureCollection({'NAME': {'foo': 1, 'baz': 2}})
    fc.read_only = True
    fcnew = FeatureCollection.loads(fc.dumps())
    assert fcnew.read_only
    with pytest.raises(ReadOnlyException):
        fcnew['NAME']['foo'] += 1


def test_read_only_not_preserved_via_dict():
    fc = FeatureCollection({'NAME': {'foo': 1, 'baz': 2}})
    fc.read_only = True
    fcnew = FeatureCollection(fc.to_dict())
    assert not fcnew.read_only
    fcnew['NAME']['foo'] += 1


def test_readonly(counter_type):
    fc = FeatureCollection({
            'hello': counter_type(Counter('hello')),
            'goodbye': counter_type(Counter('goodbye'))})
    fc2 = FeatureCollection({
            'hello': counter_type(Counter('hello')),
            'goodbye': counter_type(Counter('goodbye'))})

    fc.read_only = True
    with pytest.raises(ReadOnlyException):
        fc += fc2

    with pytest.raises(ReadOnlyException):
        fc -= fc2

    with pytest.raises(ReadOnlyException):
        fc *= 2

    with pytest.raises(ReadOnlyException):
        fc['woof'] = StringCounter()

    if hasattr(counter_type, 'read_only'):
        with pytest.raises(ReadOnlyException):
            fc['hello']['l'] = 3
        with pytest.raises(ReadOnlyException):
            fc['hello']['l'] += 3

    fc.read_only = False
    fc += fc2
    assert Counter(map(abs,fc['hello'].values())) == Counter({2: 3, 4: 1})
    fc -= fc2
    fc -= fc2
    assert Counter(map(abs,fc['hello'].values())) == Counter()
