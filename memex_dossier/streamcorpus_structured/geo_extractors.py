'''geography related extractors

.. Your use of this software is governed by your license agreement.
   Unpublished Work Copyright 2015 Diffeo, Inc.
'''
from __future__ import absolute_import
from itertools import chain
import logging

import regex as re
import geohash
import geojson
from geojson import Point, Feature

from memex_dossier.streamcorpus_structured.constants import RAW, SPAN, CANONICAL

logger = logging.getLogger(__name__)

lat_re = r'(?P<latitude>[-+]?([1-8]?\d(\.\d+)?|90(\.0+)?))'
lon_re = r'(?P<longitude>[-+]?(180(\.0+)?|((1[0-7]\d)|([1-9]?\d))(\.\d+)?))'
comma_re = r'(\s|\n)*,(\s|\n)'
latlon_regex = re.compile(lat_re + comma_re + lon_re)
#lonlat_regex = re.compile(lon_re + comma_re + lat_re) # cannot work, must pick or use fancier inference

geo_anchor_words = [u'coordinates', u'latitdue', u'longitude', u'latlon', u'coords']
geo_anchor_words_re = re.compile(r'(%s)' % '|'.join(geo_anchor_words), re.UNICODE | re.IGNORECASE)

# key parameter for precision/recall tradeoff here.  Larger slop
# window catches more coords in funky HTML formatting that spreads
# coords over multiple lines, and also introduces ore wrong
# extractions.
coord_slop_window = 2

def locations(text):
    '''parse `text` using the regexes above
    '''
    lines = text.splitlines()
    for idx, line in enumerate(lines):
        if geo_anchor_words_re.search(line):
            window = '\n'.join(lines[idx:idx + coord_slop_window])
            for match in latlon_regex.finditer(window):
                raw = match.group()
                lon = float(match.group('longitude'))
                lat = float(match.group('latitude'))
                alt = 0.0
                yield raw, match, lon, lat, alt

def locations_geohash(text):
    '''generate the RAW, CANONICAL, SPAN info for selectors
    '''
    for raw, match, lon, lat, alt in locations(text):
        ghash = geohash.encode(lat, lon) # not backwards is the API
        yield {RAW: raw, CANONICAL: ghash, SPAN: match.span()}

def locations_geojson(text):
    '''generate the RAW, CANONICAL, SPAN info for selectors with CANONICAL
    carrying [GeoJSON](http://geojson.org/) for the feature.

    For example, here is some GeoJSON:

    .. code-block:: json

       {
          "type": "Feature",
          "geometry": {
            "type": "Point",
            "coordinates": [125.6, 10.1]
          },
            "properties": {
            "name": "Dinagat Islands"
          }
       }
    '''
    for raw, match, lon, lat, alt in locations(text):
        point = Point((lon, lat))
        properties = {'name': raw}
        feature = Feature(geometry=point, properties=properties)
        yield {RAW: raw, CANONICAL: geojson.dumps(feature), SPAN: match.span()}
