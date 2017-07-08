'''`akagraph.core` tests for auxiliary functions

.. This software is released under an MIT/X11 open source license.
   Copyright 2016 Diffeo, Inc.
'''
from __future__ import absolute_import, division, print_function

import pytest

import memex_dossier.akagraph.core as core


def test_find_overlaps():
    recs = [
        {
            'phone': ['+42'],
            'name': ['chuck'],
            'email': ['car@dog.com', 'car@cat.com'],
        },{
            'phone': ['+42'],
            'name': ['bob'],
            'email': ['car@cat.com', 'other'],
        }
    ]

    expected = {
        'phone': {'+42': 2},
        'email': {'car@cat.com': 2},
    }

    info = core.find_overlaps(recs)

    assert info == expected


