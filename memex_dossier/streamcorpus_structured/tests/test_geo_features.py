'''tests for geographic structured_features

.. Your use of this software is governed by your license agreement.
   Unpublished Work Copyright 2015 Diffeo, Inc.
'''
from __future__ import absolute_import
import pytest

import geojson
import geohash

from streamcorpus_pipeline._tokenizer import nltk_tokenizer
from streamcorpus import make_stream_item

from memex_dossier.streamcorpus_structured.geo_extractors import locations
from memex_dossier.streamcorpus_structured.tests import setup_nltk, configurator


raw1 = '-90.000, -179.0000'
raw2 = '79.0000 , 44.030'
@pytest.mark.parametrize(
    ('text', 'expected_raw', 'expected_geohash'),
    [
        ('some text containing a latlon: ' + raw1, raw1, u'0008nb00j8n0'),
        ('some text containing a latlon: ' + raw2 + ' ', raw2, u'uyp33vt8m77s'),
])
def test_latlon(configurator, text, expected_raw, expected_geohash):
    """ tests latlong/geohash extraction
    """
    si = setup_nltk(text, run_extractor=True)
    selectors = si.body.selectors['structured-cyber']
    assert len(selectors) == 2
    for selector in selectors:
        assert selector.raw_selector == expected_raw

        if selector.selector_type == 'GEOHASH':
            assert selector.canonical_selector == expected_geohash

        elif selector.selector_type == 'GEOJSON':
            point = geojson.loads(selector.canonical_selector)
            lon, lat = point['geometry']['coordinates']
            assert geohash.encode(lat, lon) == expected_geohash


@pytest.yield_fixture(params=[
    ('bogus extra text not about a place', False),
    ('bogus extra text not about the location', False),
    ('bogus extra text ', False),
    ('extra text about coordinates', True),
    ('extra text about coords', True),
])
def extra_text(request):
    yield request.param


@pytest.mark.parametrize(
    ('coords_text', 'expected_lat', 'expected_lon'),
    [
        ('-89, 44', -89, 44),
        ('89, -44', 89, -44),
        ('-90.000, -179.000', -90, -179),
    ]
)
def test_locations(coords_text, expected_lat, expected_lon, extra_text):
    extra_text, should_match = extra_text
    num_matches = 0
    for raw, match, lon, lat, alt in locations(extra_text + ' ' + coords_text):
        num_matches += 1
        if should_match:
            assert lon == expected_lon
            assert lat == expected_lat
        else:
            raise Exception('should_match=%r but found %r' % (should_match, match))

    if should_match:
        assert num_matches > 0
    else:
        assert num_matches == 0
