'''memex_dossier.fc Feature Collections

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

tests to verify that StringCounter.generation changes when it should
'''
from __future__ import absolute_import, division, print_function

import pytest

from memex_dossier.fc.tests import counter_type


@pytest.fixture
def setup(counter_type):
    c1 = counter_type({'dog': 5, 'cat': 3})
    c2 = counter_type({'dog': 8, 'fish': 2})
    return (c1, c2)


def has_gen(obj):
    return hasattr(obj, 'generation')


def test_generation_iadd(setup):
    c1, c2 = setup
    if not has_gen(c1) or not has_gen(c2):
        return

    h11 = (id(c1), c1.generation)
    h21 = (id(c2), c2.generation)
    c1 += c2
    h12 = (id(c1), c1.generation)
    h22 = (id(c2), c2.generation)

    assert h11 != h12
    assert h21 == h22, c2


def test_generation_isub(setup):
    c1, c2 = setup
    if not has_gen(c1) or not has_gen(c2):
        return

    h11 = (id(c1), c1.generation)
    h21 = (id(c2), c2.generation)
    c1 -= c2
    h12 = (id(c1), c1.generation)
    h22 = (id(c2), c2.generation)

    assert h11 != h12
    assert h21 == h22, c2


def test_generation_add(setup):
    c1, c2 = setup
    if not has_gen(c1) or not has_gen(c2):
        return

    h11 = (id(c1), c1.generation)
    h21 = (id(c2), c2.generation)
    c3 = c1 + c2
    h12 = (id(c1), c1.generation)
    h22 = (id(c2), c2.generation)
    h32 = (id(c3), c3.generation)

    assert h11 == h12
    assert h11 != h32
    assert h21 == h22
    assert h21 != h32


def test_generation_pop(counter_type):
    c1 = counter_type(dict(cat=5, dog=3))
    if not has_gen(c1):
        return

    h11 = (id(c1), c1.generation)
    c1.pop('cat')
    h12 = (id(c1), c1.generation)
    assert h11 != h12, c1


def test_generation_missing(counter_type):
    c1 = counter_type(dict(cat=5, dog=3))
    if not has_gen(c1):
        return

    assert not ('bird' in c1)
    h11 = (id(c1), c1.generation)
    assert c1['bird'] == 0
    c1['bird'] += 1
    h12 = (id(c1), c1.generation)
    assert h11 != h12, c1


def test_generation_delitem(counter_type):
    c1 = counter_type(dict(cat=5, dog=3))
    if not has_gen(c1):
        return

    assert not ('bird' in c1)
    h11 = (id(c1), c1.generation)
    del c1['cat']
    h12 = (id(c1), c1.generation)
    assert h11 != h12, c1
