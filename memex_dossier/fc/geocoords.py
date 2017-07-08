'''GeoCoords carries (longitude, latitude) coordinates

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.
'''
from __future__ import absolute_import, division, print_function

from collections import MutableMapping
from itertools import ifilter, imap

from memex_dossier.fc.exceptions import uni

class GeoCoords(MutableMapping):
    '''This maps string values to lists of space-time coordinates:

        (longitude, latitude, altitude, time)

    where the spatial coordinates are expected to be Earth coordinates
    in WGS84.  `None` values *are allowed* in the four-tuples, so, for
    example, if your data lacks altitude or time, set those values to
    `None`.

    '''
    allowed_types = (float, int, long, type(None))

    def __init__(self, data=None):
        self.data = {}
        if data is not None:
            for fname, coords in data.items():
                if fname not in self.data:
                    self.data[fname] = []
                for coord in coords:
                    if not len(coord) == 4 or \
                       not all(map(lambda x: isinstance(x, self.allowed_types), 
                                   coord)):
                    # could add range checking here
                        raise Exception('expecting four-tuples of lon, '
                                        'lat, alt, time and got: %s' % repr(coord))
                    self.data[fname].append(tuple(coord))

    def to_dict(self):
        return self.data

    @staticmethod
    def from_dict(data):
        return GeoCoords(data)

    # Methods for satisfying `MutableMapping`.

    def __getitem__(self, k):
        return self.data.get(uni(k)) or self.__missing__(k)

    def __missing__(self, k):
        v = []
        self[uni(k)] = v
        return v

    def __setitem__(self, k, v):
        self.data[uni(k)] = v

    def __delitem__(self, k): 
        del self.data[uni(k)]

    def __len__(self): return len(self.data)
    def __iter__(self): return iter(self.data)

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, repr(self.data))


class GeoCoordsSerializer(object):
    '''Serialization for geocoords.'''
    def __init__(self):
        raise NotImplementedError()

    dumps = GeoCoords.to_dict
    constructor = GeoCoords

    @staticmethod
    def loads(d):
        return GeoCoords.from_dict(d)
