# -*- coding: utf-8 -*-
'''AKAGraph tests

.. This software is released under an MIT/X11 open source license.
   Copyright 2015 Diffeo, Inc.
'''

from __future__ import absolute_import
from hashlib import md5
import os
import pytest
import itertools

import memex_dossier.akagraph.core as core

# data has two connected components, to verify that they do not get
# merged accidentally (or with soft selectors that they get merged the right amount)
fake_data = [
    {
        u"url": u"a",
        u"name": [u"foo"],
        u"email": [u"foo@mail.com"],
    },{
        u"url": u"b",
        u"name": [u"кс"],
        u"skype": [u"skype1"],
        u"username": [u"username1"],
    },{
        u"url": u"c",
        u"skype": [u"skype1"],
        u"name": [u"x"],
        u"username": [u"username"],
    },
    {
        u"url": u"a2",
        u"name": [u"foo2"],
        u"email": [u"foo@mail.com2"],
    },{
        u"url": u"b2",
        u"name": [u"кс"],
        u"skype": [u"skype2"],
        u"username": [u"username"],
    },{
        u"url": u"c2",
        u"skype": [u"skype2"],
        u"name": [u"x"],
        u"username": [u"username2"],
    },
]

cc1 = {'a', 'b', 'c'}
cc2 = {'a2', 'b2', 'c2'}
truth_cc = {}
for url in cc1:
    truth_cc[url] = cc1
for url in cc2:
    truth_cc[url] = cc2

correct_hard_equivs = {
    'a': set(['b']),
    'b': set(['a', 'c']),
    'c': set(['b']),
    'a2': set(['b2']),
    'b2': set(['a2', 'c2']),
    'c2': set(['b2']),
}

def expected_equiv_strings(_url):
    _expected = set()
    cc = truth_cc[_url]
    for rec in fake_data:
        if rec['url'] in cc:
            for key, values in rec.items():
                if key == 'url': continue
                _expected.update(values)
    return _expected

def is_soft_fake_data(s):
    is_soft = None
    for rec in fake_data:
        for key, values in rec.items():
            if s in values:
                is_only_soft = bool(key in core.default_soft_selectors)
                assert is_soft is None or is_only_soft == is_soft # no ambiguity in fake_data
                is_soft = is_only_soft
    return is_soft

@pytest.yield_fixture(scope='function')
def unique_index_name():
    yield 'test_' + md5(repr(os.urandom(10))).hexdigest()

@pytest.yield_fixture(scope='function')
def populated_akagraph(unique_index_name, elastic_address):
    """ constructs an AKAGraph without any probabilistic connections
    """
    client = core.AKAGraph(
        elastic_address,
        unique_index_name,
        num_identifier_downweight=0,
        popular_identifier_downweight=0,
        hyper_edge_scorer=(lambda x: 0)
    )
    with client:
        for data in fake_data:
            client.add(data)
    # client.sync() exiting the with statement flushes which syncs
    yield client
    client.delete_index()

replica_count = 20

@pytest.yield_fixture(scope='function')
def soft_akagraph(unique_index_name, elastic_address):
    client = core.AKAGraph(
        elastic_address,
        unique_index_name,
        replicas=replica_count,
        hyper_edge_scorer=(lambda s: max(0, .5- 1.0 / len(s))),
        num_identifier_downweight=0,
        popular_identifier_downweight=0,
    )
    with client:
        for data in fake_data:
            client.add(data)
    # client.sync() exiting the with statement flushes which syncs
    yield client
    client.delete_index()


@pytest.yield_fixture(scope='function', params=fake_data)
def record(request):
    yield request.param

def test_populated(populated_akagraph, record):
    assert list(populated_akagraph.get_recs(record['url'])) == [record]

def test_raise_not_there(populated_akagraph):
    assert list(populated_akagraph.get_recs('not-there')) == [{'url': 'not-there'}]

def test_find_equivs(populated_akagraph, record):
    for rec, score, score_reason, equivs in populated_akagraph.find_equivs([record]):
        assert rec is record
        assert record['url'] not in equivs
        equivs = set(equivs)
        assert len(equivs) > 0 # true for fake_data
        if score == 1.0:
            assert equivs == correct_hard_equivs[record['url']]

def test_roots(populated_akagraph, record):
    observed_component = [url for url, count in populated_akagraph.connected_component(record['url'])]
    assert set(observed_component) == truth_cc[record['url']]

def test_get_sizes_returns(populated_akagraph):
    ids = [data['url'] for data in fake_data]
    sizes = populated_akagraph.get_sizes(*ids)
    assert isinstance(sizes, dict)

@pytest.mark.xfail # must correct the update logic
def test_get_sizes_values(populated_akagraph):
    ids = [data['url'] for data in fake_data]
    sizes = populated_akagraph.get_sizes(*ids)
    assert sizes == dict(a=3, b=1, c=1, a2=3, b2=1, c2=1)

def test_get_parent(populated_akagraph, record):
    populated_akagraph.get_parent(core.AKANode(record['url'], 0))

def test_get_children(populated_akagraph, record):
    populated_akagraph.get_children(core.AKANode(record['url'], 0))

def test_get_all_urls(populated_akagraph):
    ret = populated_akagraph.get_all_urls()
    assert len(list(ret)) == 6

def test_get_all_roots(populated_akagraph):
    ret = populated_akagraph.get_all_roots(size_limit=2, replica=0)
    del ret
    # doesn't work with probabilistic replicas
    #ret = list(ret)
    #assert len(list(ret)) == 2

def test_connected_component(populated_akagraph):
    assert set('abc') == set([url for url, count in populated_akagraph.connected_component('a')])

#def test_find_perf(populated_akagraph, record):
#    equivs = list(populated_akagraph.find_equivs(record))
#    assert len(equivs) > 0

def test_find_equivs_specific(populated_akagraph):
    # check the a=b U b=c structure of fake_data
    def get_one(i):
        _, _, _, equivs = list(populated_akagraph.find_equivs([fake_data[i]]))[0]
        return list(equivs)
    assert fake_data[0]['url'] == 'a'
    assert fake_data[1]['url'] == 'b'
    assert fake_data[2]['url'] == 'c'
    assert ['b'] == get_one(0)
    assert ['b'] == get_one(2)
    assert set(['a', 'c']) == set(get_one(1))

def test_set_parent(populated_akagraph):
    for replica in populated_akagraph.replica_list:
        a = core.AKANode('a', replica)
        b2 = core.AKANode('b2', replica)
        populated_akagraph.set_parents(a, b2)
        populated_akagraph.sync()
        assert populated_akagraph.get_parent(b2) == a

def test_unite(populated_akagraph):
    for replica in populated_akagraph.replica_list:
        nodes = [core.AKANode(rec['url'], replica) for rec in fake_data]
        root = populated_akagraph.unite(*nodes)
        nodes.remove(root)
        populated_akagraph.sync()
        for node in nodes:
            assert root == populated_akagraph.get_root(node)

@pytest.mark.xfail
def test_find(populated_akagraph, record):
    # verify that ingest actually found and united the three; requires
    # `sync` call in the middle of the two-stage ingest process in
    # `flush` method.
    assert populated_akagraph.find(record['url'], truth_cc[record['url']])
    assert False

def test_find_roots_by_selector(populated_akagraph, record):
    for key, values in record.items():
        if key == 'url': continue
        if key in set(core.default_soft_selectors): continue
        for val in values:
            equivs = set(populated_akagraph.find_urls_by_selector(val))
            assert equivs.issubset(truth_cc[record['url']])

@pytest.mark.xfail
def test_find_connected_component(populated_akagraph, record):
    expected_cc = truth_cc[record['url']]
    for key, values in record.items():
        if key == 'url': continue
        if key in set(core.default_soft_selectors): continue
        for val in values:
            ccs = populated_akagraph.find_connected_component(val)
            ccs = list(ccs)
            assert len(ccs) == 1
            assert expected_cc == set([rec['url'] for rec in ccs[0]])

@pytest.mark.xfail
def test_analyze_clusters(populated_akagraph):
    ret = populated_akagraph.analyze_clusters()
    assert len(ret['clusters']) == 2
    assert ret['clusters'][0]['count'] == 3
    assert ret['clusters'][1]['count'] == 3



#def test_soft_clusters(soft_akagraph, record):
def test_soft_clusters(soft_akagraph, record):
    ''' note that this test depends closely on the structure of the data being ingested
    as well as upon the (faked for this example) function used to score the similarity
    of the soft selectors
    '''
    low_bound = replica_count * .3
    high_bound = replica_count * .55
    always_cc = truth_cc[record['url']]
    soft_akagraph.sync()
    cc = list(soft_akagraph.connected_component(record['url']))
    othercount=None
    for name, count in cc:
        if name in always_cc:
            assert count == replica_count
        else:
            if othercount:
                assert othercount == count
            else:
                othercount = count
                assert low_bound <= othercount <= high_bound

def test_add_edge(soft_akagraph):
    '''
    this takes the existing soft graph which looks like:
    (a, b, c) loosely bound to (a2, b2, c3) 
    and adds the following:
    a bound to a2 with weight .2 and evidence 'username'.  
    Since 'username' is already binding the clusters, verify that this does not change anything.
    b bound to b2 with no evidence once, verify this increases the counts
    b bound to b2 again with no evidence, verify this further increases the counts
    c bound to c2 with strength 1, verify clusters are now completely connected.

    use new records d, e, f, g and bind them sequentially with strength .2 and the same evidence
    verify that they are all connected ~.2
    bind d, e, f, g sequentially with strength .6 and different evidence
    verify connectivities go up, but drop off with distance.
    '''

    # to make tests deterministic, we overwrite core.uniform_random using a loop over the
    # following list which was generated from [round(random.uniform(0, 1), 4) for i in range(64)]
    pseudorandom = itertools.cycle([
        0.0607, 0.1474, 0.037, 0.9118, 0.353, 0.3549, 0.4509, 0.6694, 0.6033, 0.0424,
        0.2345, 0.0005, 0.6237, 0.647, 0.1401, 0.6782, 0.3207, 0.6538, 0.7681, 0.4805,
        0.4934, 0.3134, 0.2269, 0.9257, 0.9147, 0.4915, 0.2829, 0.2345, 0.1139, 0.9229, 
        0.7149, 0.6885, 0.2881, 0.4665, 0.3018, 0.3008, 0.4896, 0.5462, 0.3342, 0.2676,
        0.9385, 0.1902, 0.1997, 0.7592, 0.6843, 0.5238, 0.7127, 0.5245, 0.1827, 0.7681,
        0.5855, 0.4648, 0.0723, 0.7006, 0.1429, 0.1367, 0.7325, 0.4641, 0.6702, 0.7616,
        0.1671, 0.3075, 0.7183, 0.4821
    ])
    core.uniform_random = pseudorandom.next

    def get_counts(from_id):
        ret = dict(soft_akagraph.connected_component(from_id))
        ret.pop(from_id)
        return ret

    def verify_dict_values_equal(*dicts):
        s = set()
        for d in dicts:
            s.update(d.values())
        return len(s) == 1

    base_count = get_counts('a')['a2']

    with soft_akagraph:
        # this is the same evidence already connecting the components, so it shouldn't change counts
        soft_akagraph.add_edge(['a', 'a2'], .3, 'username')

    assert base_count == get_counts('b')['b2']

    with soft_akagraph:
        # make a weak link with independent evidence, it should increase counts
        soft_akagraph.add_edge(['b', 'b2'], .3)

    new_count1 = get_counts('a')['c2']
    assert new_count1 > base_count

    with soft_akagraph:
        # make another weak link with independent evidence, it should increase counts more
        soft_akagraph.add_edge(['b', 'b2'], .3)

    new_count2 = get_counts('a')['c2']
    assert new_count2 > new_count1

    with soft_akagraph:
        # make a 100% link with independent evidence, it should max out counts
        soft_akagraph.add_edge(['c', 'c2'], 1)

    new_count3 = get_counts('a')['c2']
    assert new_count3 == replica_count # at this point the graph is completely connected

    with soft_akagraph:
        # these edges all have the same evidence, so the connectivity is transitive
        soft_akagraph.add_edge(['d', 'e'], .2, 'evidence1')
        soft_akagraph.add_edge(['e', 'f'], .2, 'evidence1')
        soft_akagraph.add_edge(['f', 'g'], .2, 'evidence1')

    assert verify_dict_values_equal(get_counts('e'), get_counts('f'), get_counts('g'))

    with soft_akagraph:
        # these counts have unspecified evidence (equivalent to different) and so are indepenent
        soft_akagraph.add_edge(['d', 'e'], .7)
        soft_akagraph.add_edge(['e', 'f'], .7)
        soft_akagraph.add_edge(['f', 'g'], .7)

    counts_d = get_counts('d')
    counts_e = get_counts('e')

    assert counts_d['e'] >= counts_d['f'] >= counts_d['g']  # counts decrease with distance
    assert counts_d['e'] > counts_d['g']  # e and g are far enough appart counts strictly decrease
    assert counts_d['e'] == counts_e['d']  # count d->e == count e->d
    assert counts_e['f'] >= counts_e['g']  # counts decrease (from e) with distance

    # make sure adding a record for 'h' with the union=False doesn't connect it to a,b,c
    with soft_akagraph:
        soft_akagraph.add({
            u"url": u"h",
            u"name": [u"foo"],
            u"email": [u"foo@mail.com"],
        }, analyze_and_union=False)

    counts_h = get_counts('h')
    assert not counts_h
