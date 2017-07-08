'''memex_dossier.fc Feature Collections

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

Tests for counters which require support for `__missing__` in
FeatureCollection.
'''
from __future__ import absolute_import, division, print_function
try:
    from collections import Counter
except ImportError:
    from backport_collections import Counter

import pytest

from memex_dossier.fc import FeatureCollection, StringCounter, SerializationError
from memex_dossier.fc.tests import counter_type


def assert_same_fc(fc1, fc2):
    '''Verify that fc1 and fc2 have the same data, otherwise raise'''

    ## verify the keys have not changed
    keys1 = set(fc1.keys())
    keys2 = set(fc2.keys())
    assert keys1 == keys2, keys2 - keys1

    ## verify the vals ahve not changed
    vals1 = set()
    for mset in fc1.values():
        map(vals1.add, mset.items())
    vals2 = set()
    for mset in fc2.values():
        map(vals2.add, mset.items())
    assert vals1 == vals2, vals1 - vals2


def test_bundle_default(counter_type):
    'does it make a new counter that adds properly'
    fc = FeatureCollection()
    assert isinstance(fc['foo'], counter_type)

    fc['foo'] += counter_type(Counter('dog'))
    assert isinstance(fc['foo'], counter_type), \
        'failed and made %s' % type(fc['foo'])

    fc['foo'] -= counter_type(Counter('dog'))
    assert isinstance(fc['foo'], counter_type), \
        'failed and made %s' % type(fc['foo'])

    if hasattr(fc['foo'], 'substract'):
        fc['foo'].subtract(counter_type(Counter('dog')))
        assert isinstance(fc['foo'], counter_type), \
            'failed and made %s' % type(fc['foo'])
        fc['foo'] += counter_type(Counter('dog'))

    fc['foo'] += counter_type(Counter('dog'))
    assert isinstance(fc['foo'], counter_type), \
        'failed and made %s' % type(fc['foo'])
    fc['foo'] += counter_type(Counter('dog'))
    fc['foo'] += counter_type(Counter('dog cat'))
    assert Counter(map(abs,fc['foo'].values())) == Counter({1: 4, 3: 3})


def test_fc_build_from_dict(counter_type):
    fc = FeatureCollection({
            'hello': counter_type(Counter('hello')),
            'goodbye': counter_type(Counter('goodbye'))})
    assert Counter(map(abs,fc['hello'].values())) == Counter({1: 3, 2: 1})
    assert isinstance(fc['hello'], counter_type)


def test_fc_meta_adding(counter_type):
    fc = FeatureCollection({
            'hello': counter_type(Counter('hello')),
            'goodbye': counter_type(Counter('goodbye'))})
    fc2 = fc + fc
    assert Counter(map(abs,fc2['hello'].values())) == Counter({2: 3, 4: 1})


def test_fc_eq(counter_type):
    fc1 = FeatureCollection({
            'hello': counter_type(Counter('hello')),
            'goodbye': counter_type(Counter('goodbye'))})
    fc2 = FeatureCollection({
            'hello': counter_type(Counter('hello')),
            'goodbye': counter_type(Counter('goodbye'))})
    fc3 = FeatureCollection({
            'hello': counter_type(Counter('hello')),
            'goodbye': counter_type(Counter('goodbye2'))})

    assert fc1 == fc2
    assert fc1 != fc3


def test_fc_meta_adding_complex(counter_type):
    fc = FeatureCollection({
            'hello': counter_type(Counter('hello')),
            'goodbye': counter_type(Counter('goodbye'))})
    fc2 = FeatureCollection({
            'hello': counter_type(Counter('hello')),
            'goodbye': counter_type(Counter('goodbye'))})
    fc3 = fc + fc2

    assert Counter(map(abs,fc3['hello'].values())) == Counter({2: 3, 4: 1})
    fc += fc2
    assert Counter(map(abs,fc['hello'].values())) == Counter({2: 3, 4: 1})

    fc3 -= fc2
    assert Counter(map(abs,fc3['hello'].values())) == Counter({1: 3, 2: 1})

    fc3 -= fc2
    assert Counter(map(abs,fc3['hello'].values())) == Counter()


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


def test_values(counter_type):
    c = counter_type(Counter('dog'))
    assert set(map(abs,map(int, c.values()))) == set([1])
    assert len(c.values()) == 3


def test_entity(counter_type):
    ## build entity, serialize, deserialize, and verify its multisets
    fc1 = FeatureCollection()
    fc1['bow'] += counter_type(Counter(['big', 'dog']))
    fc1['bow'] += counter_type(Counter('tall building'))
    fc1['bon'] += counter_type(Counter(['Super Cat', 'Small Cat', 'Tiger Fish']))

    ## there should be nine items of size 1
    assert Counter(map(abs,fc1['bow'].values()))[1] == 10, fc1['bow'].items()

    ## double the counts, should recurse down
    fc1 += fc1

    ## check values doubled
    assert Counter(map(abs,fc1['bow'].values()))[2] == 10, fc1['bow'].items()

    ## serialize/deserialize it
    blob = fc1.dumps()
    assert_same_fc(fc1, FeatureCollection.loads(blob))

    ## deserialize it via chunk
    fc2 = FeatureCollection.loads(fc1.dumps())
    assert_same_fc(fc1, fc2)


def test_multiset_equality(counter_type):
    ent1 = FeatureCollection()
    ent2 = FeatureCollection()
    ent1['bow'] += counter_type(Counter(['big', 'dog']))
    ent2['bow'] += counter_type(Counter(['big', 'dog']))
    assert ent1 == ent2


def test_multiset_change(counter_type):
    ent1 = FeatureCollection()
    ent1['bow'] += counter_type(Counter(['big', 'dog']))
    ent1.pop('bow')
    assert dict(ent1.items()) == dict()

    ## can pop empty -- fails
    #ent1.pop('foo')

    ## set equal to
    test_data = ['big2', 'dog2']
    ent1['bow'] = counter_type(Counter(test_data))
    assert list(map(abs,ent1['bow'].values())) == [1,1]

    ent1['bow'] += counter_type(Counter(test_data))
    assert list(map(abs,ent1['bow'].values())) == [2,2]


def test_serialize_deserialize(counter_type):
    ## build entity, serialize, deserialize, and verify its multisets
    ent1 = FeatureCollection()
    ent1['bow'] += counter_type(Counter(['big', 'dog']))
    ent1['bow'] += counter_type(Counter('tall building'))
    ent1['bon'] += counter_type(Counter(['Super Cat', 'Small Cat',
                                         'Tiger Fish']))

    blob = ent1.dumps()
    ent2 = FeatureCollection.loads(blob)
    assert_same_fc(ent1, ent2)


def test_type(counter_type):
    ent1 = FeatureCollection()
    ent1['bow'] += counter_type(Counter(['big', 'dog']))
    if counter_type.__name__ == 'StringCounter':
        ent1['bow']['a'] += 1
    assert isinstance(ent1, FeatureCollection)

    ent3 = FeatureCollection()
    ent3['bow'] += counter_type(Counter(['cat']))

    ent1 += ent3
    assert isinstance(ent1, FeatureCollection)


def test_clone(counter_type):
    ent1 = FeatureCollection()
    ent1['bow'] += counter_type(Counter(['cat']))
    ent2 = FeatureCollection(ent1)
    assert ent2 is not ent1
    assert ent2 == ent1


def test_non_counter_features_serialize():
    fc = FeatureCollection({'NAME': u'foobaz'})
    fc = FeatureCollection.loads(fc.dumps())


def test_non_counter_features_bad_serialize():
    with pytest.raises(SerializationError):
        FeatureCollection({'NAME': 'foobaz'})
    fc = FeatureCollection()
    fc['NAME'] = 'foobaz'
    with pytest.raises(SerializationError):
        fc.dumps()


def test_non_counter_features_total():
    fc = FeatureCollection({'NAME': u'foobaz'})
    assert fc.total() == 0


def test_non_counter_features_inequality():
    fc1 = FeatureCollection({'NAME': u'foobaz'})
    fc2 = FeatureCollection({'NAME': u'blahblahblah'})
    assert fc1 != fc2


def test_binop_no_share():
    fc1 = FeatureCollection({'NAME': {'foo': 1, 'bar': 1}})
    fc2 = FeatureCollection({'NAME': {'foo': 2, 'bar': 2}})

    fc3 = fc1 + fc2

    assert fc1['NAME']['foo'] == 1
    assert fc2['NAME']['foo'] == 2

    fc1 += fc2
    assert fc1 == fc3
    assert fc1['NAME']['foo'] == 3
    assert fc2['NAME']['foo'] == 2


def test_binop_different_no_share():
    fc1 = FeatureCollection({'FOO': {'foo': 1}})
    fc2 = FeatureCollection({'BAR': {'bar': 1}})

    result = fc1 + fc2
    expected = FeatureCollection({'FOO': {'foo': 1 }, 'BAR': {'bar': 1}})
    assert result == expected

    result['BAR']['bar'] = 2
    assert fc2['BAR']['bar'] == 1

    result['FOO']['foo'] = 2
    assert fc1['FOO']['foo'] == 1


def test_subtraction():
    fc1 = FeatureCollection({'a': {'a': 3, 'b': 2}, 'b': {'b': 2}})
    fc2 = FeatureCollection({'a': {'a': 3, 'c': 1}, 'c': {'c': 1}})
    fc3 = fc1 - fc2

    assert fc3 == FeatureCollection({'a': {'b': 2}, 'b': {'b': 2}, 'c': {}})


def test_get():
    fc = FeatureCollection()
    assert fc.get('nada', 5) == 5
