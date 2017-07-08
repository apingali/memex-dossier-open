'''memex_dossier.fc Feature Collections

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

tests for FeatureCollection which has __missing__ that detects which kind
of counter is active.
'''
from __future__ import absolute_import, division, print_function
try:
    from collections import Counter
except ImportError:
    from backport_collections import Counter

from memex_dossier.fc import FeatureCollection, StringCounter

from memex_dossier.fc.tests import counter_type


def test_default(counter_type):
    'does a FC make a new counter that adds properly'
    mc = FeatureCollection()
    assert isinstance(mc['foo'], counter_type)

    mc['foo'] += counter_type(Counter('dog'))
    assert isinstance(mc['foo'], counter_type), \
        'failed and made %s' % type(mc['foo'])

    mc['foo'] -= counter_type(Counter('dog'))
    assert isinstance(mc['foo'], counter_type), \
        'failed and made %s' % type(mc['foo'])

    if hasattr(mc['foo'], 'substract'):
        mc['foo'].subtract(counter_type(Counter('dog')))
        assert isinstance(mc['foo'], counter_type), \
            'failed and made %s' % type(mc['foo'])
        mc['foo'] += counter_type(Counter('dog'))

    mc['foo'] += counter_type(Counter('dog'))
    assert isinstance(mc['foo'], counter_type), \
        'failed and made %s' % type(mc['foo'])

    mc['foo'] += counter_type(Counter('dog'))
    mc['foo'] += counter_type(Counter('dog cat'))
    assert Counter(map(abs,mc['foo'].values())) == Counter({1: 4, 3: 3})


def test_build_from_dict(counter_type):
    mc = FeatureCollection({
            'hello': counter_type(Counter('hello')), 
            'goodbye': counter_type(Counter('goodbye'))})

    assert Counter(map(abs,mc['hello'].values())) == Counter({1: 3, 2: 1})
    assert isinstance(mc['hello'], counter_type)


def test_meta_adding(counter_type):
    mc = FeatureCollection({
            'hello': counter_type(Counter('hello')),
            'goodbye': counter_type(Counter('goodbye'))})
    mc2 = mc + mc

    assert Counter(map(abs,mc2['hello'].values())) == Counter({2: 3, 4: 1})


def test_eq(counter_type):
    mc1 = FeatureCollection({
            'hello': counter_type(Counter('hello')),
            'goodbye': counter_type(Counter('goodbye'))})
    mc2 = FeatureCollection({
            'hello': counter_type(Counter('hello')),
            'goodbye': counter_type(Counter('goodbye'))})
    mc3 = FeatureCollection({
            'hello': counter_type(Counter('hello')),
            'goodbye': counter_type(Counter('goodbye2'))})

    assert mc1 == mc2
    assert mc1 != mc3
    

def test_meta_adding_complex(counter_type):
    mc = FeatureCollection({
            'hello': counter_type(Counter('hello')), 
            'goodbye': counter_type(Counter('goodbye'))})
    mc2 = FeatureCollection({
            'hello': counter_type(Counter('hello')),
            'goodbye': counter_type(Counter('goodbye'))})
    mc3 = mc + mc2

    assert Counter(map(abs,mc3['hello'].values())) == Counter({2: 3, 4: 1})
    mc += mc2
    assert Counter(map(abs,mc['hello'].values())) == Counter({2: 3, 4: 1})

    ## isub tests
    mc3 -= mc2
    assert Counter(map(abs,mc3['hello'].values())) == Counter({1: 3, 2: 1})
    
    mc3 -= mc2
    assert Counter(map(abs,mc3['hello'].values())) == Counter()


def test_type(counter_type):
    m1 = FeatureCollection()
    m1['bow'] += counter_type(Counter(['big', 'dog']))

    assert type(m1) == FeatureCollection

    m2 = FeatureCollection()
    m2['bow'] += counter_type(Counter(['cat']))
    m1 += m2

    assert type(m1) == FeatureCollection


def test_string_counter():
    hc = StringCounter('this sentence has been parsed')
    h1 = (id(hc), hc.generation)

    cache = dict(h1='hi')
    assert len(cache) == 1

    hc.update('more text')
    h2 = (id(hc), hc.generation)
    cache[h2] = 'hi again?'

    assert len(cache) == 2
    assert h1 != h2


def test_string_counter2():
    sc = StringCounter()
    sc['hi'] += 1
    assert isinstance(sc, StringCounter)
    sc += Counter('hi')
    assert isinstance(sc, StringCounter)


def test_unicode():
    fc = FeatureCollection({'NAME': {'foo': 1}})
    fc = FeatureCollection.loads(fc.dumps())
    assert type(fc.keys()[0]) is unicode
    assert type(fc['NAME'].keys()[0]) is unicode
