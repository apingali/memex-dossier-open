'''memex_dossier.fc Feature Collections

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

'''

from __future__ import absolute_import, division, print_function

import pytest

from memex_dossier.fc.feature_collection import registry, FeatureTypeRegistry


def is_testable_counter(k):
    return 'Counter' in k or k == 'SparseVector'


@pytest.yield_fixture(params=filter(is_testable_counter, registry.types()))
def counter_type(request):
    ct = registry.get_constructor(request.param)
    old_default = FeatureTypeRegistry.DEFAULT_FEATURE_TYPE_NAME
    FeatureTypeRegistry.DEFAULT_FEATURE_TYPE_NAME = request.param
    yield ct
    FeatureTypeRegistry.DEFAULT_FEATURE_TYPE_NAME = old_default
