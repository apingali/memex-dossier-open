'''tests for GeoCoords

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.
'''

from memex_dossier.fc import FeatureCollection
from memex_dossier.fc.geocoords import GeoCoords, GeoCoordsSerializer



def test_geo_fcdefault():
    fc = FeatureCollection()
    assert isinstance(fc['!co_LOC'], GeoCoords)

def test_geo_default():
    fo = GeoCoords()
    assert fo['foo'] == []


def test_geo_roundtrip():
    fc = FeatureCollection()
    fc['!co_LOC']['foo'].append((-55, 22, 0, None))
    fc2 = FeatureCollection.loads(fc.dumps())
    assert fc['!co_LOC'] == fc2['!co_LOC']

def test_geocoords():
    data = {'Boston': [(-72, 44, 2, None), (99, -22., None, 1434218285)]}

    geo = GeoCoords(data)

    out = GeoCoordsSerializer.dumps(geo)
    
    geo2 = GeoCoordsSerializer.loads(out)

    assert geo is not geo2
    assert geo == geo2
