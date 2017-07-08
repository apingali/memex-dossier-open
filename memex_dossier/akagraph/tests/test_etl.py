'''AKAGraph tests

.. This software is released under an MIT/X11 open source license.
   Copyright 2016 Diffeo, Inc.
'''
from __future__ import absolute_import, division, print_function

import pytest

import memex_dossier.akagraph.etl as etl

@pytest.mark.parametrize(
    'i_field, o_field, i_name,o_name',
    [
        ('attn', 'name', 'SMITH,BOB', 'Bob Smith'),
    ]
)
def test_company_abbr(i_field, o_field, i_name, o_name):
    hard_selectors = set()
    name = etl.normalize(i_field, o_field, i_name, hard_selectors)
    assert name == o_name


@pytest.mark.parametrize(
    'phone,is_bad',
    [
        ("+8675500000000", True),
        ("+8675511111111", True),
        ("+8675512345678", True),
        ("+8675527801220", False),
        ("+8675527838090", False),
        ("+8675533668888", False),
        ("+8675581878034", False),
        ("+8675583725876", False),
        ("+8675584291218", False),
        ("+8675587654321", True),
        ("+8675528888888", True),
        ("+8675566666666", False),
        ("+8675584825856", False),
        ("+8675586868686", False),
        ("+8675588888888", True),
    ]
)
def test_is_bad_phone_number(phone, is_bad):
    assert is_bad == etl.is_bad_phone_number(phone)


