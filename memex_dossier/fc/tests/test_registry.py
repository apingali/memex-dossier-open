'''memex_dossier.fc Feature Collections

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

'''
from __future__ import absolute_import, division, print_function

import pytest

from memex_dossier.fc.feature_collection import registry
from memex_dossier.fc.string_counter import StringCounterSerializer


def test_unknown_feature_type_name():
    with registry:
        with pytest.raises(ValueError):
            registry.add('Unknown', StringCounterSerializer)


def test_unknown_feature_type():
    class Blah(object):  # does not inherit from known feature type
        pass

    class Serializer(object):
        constructor = Blah

    with registry:
        with pytest.raises(ValueError):
            registry.add('StringCounter', Serializer)
