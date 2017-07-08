'''Collections of named features.

This module provides :class:`memex_dossier.fc.FeatureCollection` and a
number of supporting classes.  A feature collection is a dictionary
mapping a feature name to a feature representation.  The
representation is typically a :class:`memex_dossier.fc.StringCounter`, an
implementation of :class:`collections.Counter`, which is fundamentally
a mapping from a string value to an integer.

This representation allows multiple values to be stored for multiple
bits of information about some entity of interest.  The weights in the
underlying counters can be used to indicate how common a particular
value is, or how many times it appears in the source documents.

    .. code-block:: python

        president = FeatureCollection()
        president['NAME']['Barack Obama'] += 1
        president['entity_type']['PER'] += 1
        president['PER_ADDRESS']['White House'] += 1
        president['PER_ADDRESS']['1600 Pennsylvania Ave.'] += 1

Feature collections can have representations other than the basic
string-counter representation; these representations may not preserve
the source strings, but will still be suitable for machine-learning
applications.

Feature collections can be serialized to :rfc:`7049` CBOR format,
similar to a binary JSON representation.  They can also be stored
sequentially in flat files using
:class:`memex_dossier.fc.FeatureCollectionChunk` as an accessor.

.. autoclass:: FeatureCollection
   :show-inheritance:

.. autoclass:: StringCounter
   :show-inheritance:

.. autoclass:: SparseVector
   :show-inheritance:

.. autoclass:: DenseVector
   :show-inheritance:

.. autoclass:: FeatureCollectionChunk
   :show-inheritance:

.. autoclass:: ReadOnlyException
   :show-inheritance:

.. autoclass:: SerializationError
   :show-inheritance:

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

'''
from memex_dossier.fc.exceptions import ReadOnlyException, SerializationError
from memex_dossier.fc.feature_collection import \
    FeatureCollection, FeatureCollectionChunk
from memex_dossier.fc.feature_tokens import FeatureTokens
from memex_dossier.fc.string_counter import StringCounter
from memex_dossier.fc.geocoords import GeoCoords
from memex_dossier.fc.vector import DenseVector, SparseVector

__all__ = [
    'FeatureCollection', 'FeatureCollectionChunk',
    'StringCounter', 'SparseVector', 'DenseVector', 'FeatureTokens',
    'ReadOnlyException', 'SerializationError',
]
