from __future__ import absolute_import, division, print_function

import json
from cStringIO import StringIO
import urllib

import bottle

from memex_dossier.fc import FeatureCollection
import memex_dossier.web.routes as routes
from memex_dossier.web.tests import config_local, kvl, label_store  # noqa


def rot14(s):
    # Use `rot14` so that `rot14(rot14(s)) != s`.
    return ''.join(chr(ord('a') + ((ord(c) - ord('a')) + 14) % 26) for c in s)


def dbid_to_visid(s):
    return rot14(s)


def visid_to_dbid(s):
    return rot14(s)


def new_request(params=None, body=None):
    environ = {'wsgi.input': StringIO('')}
    if params is not None:
        environ['QUERY_STRING'] = urllib.urlencode(params)
    if body is not None:
        environ['wsgi.input'] = StringIO(body)
        environ['CONTENT_LENGTH'] = len(body)
    return bottle.Request(environ=environ)


def new_response():
    return bottle.Response()

