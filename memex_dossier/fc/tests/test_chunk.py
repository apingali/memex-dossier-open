'''memex_dossier.fc Feature Collections

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

'''
from __future__ import absolute_import, division, print_function

from StringIO import StringIO

from memex_dossier.fc import FeatureCollection, FeatureCollectionChunk

def test_fc_chunk():
    fc1 = FeatureCollection({'NAME': {'foo': 2, 'baz': 1}})
    fc2 = FeatureCollection({'NAME': {'foo': 4, 'baz': 2}})

    fh = StringIO()
    chunk = FeatureCollectionChunk(file_obj=fh, mode='wb')
    chunk.add(fc1)
    chunk.add(fc2)
    chunk.flush()

    blob = fh.getvalue()
    assert blob
    fh = StringIO(blob)
    chunk = FeatureCollectionChunk(file_obj=fh, mode='rb')
    rfc1, rfc2 = list(chunk)
    assert fc1 == rfc1
    assert fc2 == rfc2
