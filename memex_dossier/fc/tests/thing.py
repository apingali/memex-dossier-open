'''Serializable feature-like thing for tests.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

'''
from __future__ import absolute_import, division, print_function
import json

from memex_dossier.fc import StringCounter


class Thing(StringCounter):
    def __init__(self, blob=None):
        self.data = dict()
        if blob is not None:
            self.loads(blob)

    def dumps(self):
        return json.dumps(self.data)

    def loads(self, blob):
        self.data = json.loads(blob)

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = value

    def do_more_things(self):
        self.data['doing'] = 'something'

    def __eq__(self, other):
        return self.data == other.data

    def __repr__(self):
        return 'Thing({!r})'.format(self.dumps())


class ThingSerializer(object):
    def __init__(self):
        raise NotImplementedError

    @staticmethod
    def loads(blob):
        return Thing(blob)

    @staticmethod
    def dumps(thing):
        return thing.dumps()

    constructor = Thing
