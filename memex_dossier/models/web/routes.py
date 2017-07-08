'''web service endpoints for supporting SortingDesk

.. This software is released under an MIT/X11 open source license.
   Copyright 2015 Diffeo, Inc.
'''
from __future__ import absolute_import, division, print_function

import datetime
import email.utils as eut
#try:
#    from cStringIO import StringIO
#except ImportError:
#    from StringIO import StringIO
from hashlib import md5
import logging
import os.path as path
from operator import itemgetter
import time
import traceback
import urllib

import bottle
import json
from nilsimsa import Nilsimsa
from streamcorpus_pipeline import cleanse
from streamcorpus_pipeline._clean_html import uniform_html
from streamcorpus_pipeline.offsets import char_offsets_to_xpaths
import regex as re
import requests

import dblogger
from memex_dossier.fc import StringCounter, FeatureCollection
from memex_dossier.models import etl
from memex_dossier.models.web.config import Config
from memex_dossier.models.recommend import get_recommendations
from memex_dossier.streamcorpus_structured.cyber_extractors import phonenumber_matcher
from memex_dossier.web.util import fc_to_json
import kvlayer
import coordinate
from coordinate.constants import \
    AVAILABLE, BLOCKED, PENDING, FINISHED, FAILED
import yakonfig


app = bottle.Bottle()
logger = logging.getLogger(__name__)
web_static_path = path.join(path.split(__file__)[0], 'static')
bottle.TEMPLATE_PATH.insert(0, path.join(web_static_path, 'tpl'))

@app.put('/dossier/v1/feature-collection/<cid>', json=True)
def v1_fc_put(request, response, store, kvlclient, tfidf, cid):
    '''Store a single feature collection.

    The route for this endpoint is:
    ``PUT /dossier/v1/feature-collections/<content_id>``.

    ``content_id`` is the id to associate with the given feature
    collection. The feature collection should be in the request
    body serialized as JSON.

    Alternatively, if the request's ``Content-type`` is
    ``text/html``, then a feature collection is generated from the
    HTML. The generated feature collection is then returned as a
    JSON payload.

    This endpoint returns status ``201`` upon successful
    storage otherwise. An existing feature collection with id
    ``content_id`` is overwritten.
    '''
    tfidf = tfidf or None
    if request.headers.get('content-type', '').startswith('text/html'):
        url = urllib.unquote(cid.split('|', 1)[1])
        fc = etl.create_fc_from_html(url, request.body.read(), tfidf=tfidf)
        logger.info('created FC for %r', cid)
        store.put([(cid, fc)])
        return fc_to_json(fc)
    else:
        fc = FeatureCollection.from_dict(json.load(request.body))
        keywords = set()
        for subid in fc:
            if subid.startswith('subtopic'):
                ty = subtopic_type(subid)
                if ty in ('text', 'manual'):
                    # get the user selected string
                    data = typed_subtopic_data(fc, subid)
                    map(keywords.add, cleanse(data).split())
                    keywords.add(cleanse(data))

        folders = Folders(kvlclient)
        for fid, sid in folders.parent_subfolders(cid):
            if not isinstance(fid, unicode):
                fid = fid.decode('utf8')
            if not isinstance(sid, unicode):
                sid = sid.decode('utf8')
            keywords.add(cleanse(fid))
            keywords.add(cleanse(sid))

        fc[u'keywords'] = StringCounter(keywords)
        store.put([(cid, fc)])
        response.status = 201

        #return routes.v1_fc_put(request, response, lambda x: x, store, cid)

post_akagraph_edge_usage = '[{"urls": [<string>, <string>, ...], (optional args) "strength": <int>, "evidence": <string>}, ...]'

@app.post('/dossier/v1/akagraph_edge')
def v1_akagraph_post_edge(request, response, akagraph):
    try:
        request.json
    except ValueError:
        response.status = 400
        return {"error": "request body must be valid json"}

    if isinstance(request.json, list):
        edges = request.json
    elif isinstance(request.json, dict):
        edges = [ request.json ]
    else:
        response.status = 400
        return {"usage": post_akagraph_edge_usage}

    processed_count = 0
    with akagraph:
        for edge in edges:
            if (isinstance(edge, dict) and 'urls' in edge 
                and isinstance(edge['urls'], list)):
                urls = edge['urls']
                strength = edge.get('strength', 1.0)
                if not isinstance(strength, (int, float, long)):
                    continue
                evidence = edge.get('evidence', None)
                if evidence and not isinstance(evidence, str):
                    continue
                processed_count += 1
                akagraph.add_edge(urls, strength, evidence)

    response_dict = {}
    if processed_count < len(edges):
        response_dict['warning'] = ('only %s of %s records were had a "urls" field and were processed as valid' 
                                    % (processed_count, len(edges)))
    if response_dict:
        response_dict['usage'] = post_akagraph_edge_usage

    return response_dict

post_akagraph_record_usage = '[{"url": <string>, <string>: [<string>, ... ], ... }, ... ]'

@app.post('/dossier/v1/akagraph')
def v1_akagraph_post_record(request, response, akagraph):
    try:
        request.json
    except ValueError:
        response.status = 400
        return {"error": "request body must be valid json"}

    if isinstance(request.json, list):
        records = request.json
    elif isinstance(request.json, dict):
        records = [request.json]
    else:
        # throw a better error?
        response.status = 400
        return {"usage": post_akagraph_record_usage}

    analyze_and_union = 'analyze' in request.query

    processed = set()
    processed_count = 0
    with akagraph:
        for record in records:
            # check that the record is legit (it has a 'url' key)
            if 'url' in record:
                processed.add(record['url'])
                processed_count += 1
                akagraph.add(record, analyze_and_union)
            else:
                continue
    response_dict = {}
    if processed_count != len(processed):
        response_dict['warning'] = '%s unique urls out of %s' % (len(processed), processed_count)
    if len(processed) < len(records):
        response_dict['error'] = ('only %s out of %s records were processed as valid (contained a "url" field)' 
                                  % (processed_count, len(records)))
    if response_dict:
        response_dict['usage'] = post_akagraph_record_usage
    response_dict['urls'] = list(processed)
    return response_dict

@app.get('/dossier/v1/akagraph')
def v1_akagraph_get(request, response, akagraph):
    recs = akagraph.get_recs(request.query.url)
    rec = list(recs)[0]
    response.content_type = 'text/html; charset=utf-8'
    if not 'text' in rec:
        return 'no text in record\n\n<pre>' + \
            json.dumps(rec, indent=4, sort_keys=True) + \
            '</pre>'
    else:
        return rec.pop('text') + '\n\n<pre>' + \
            json.dumps(rec, indent=4, sort_keys=True) + \
            '</pre>'



@app.get('/dossier/v1/suggest/<query:path>', json=True)
def v1_suggest_get(request, response, tfidf, akagraph, query):
    '''Gather suggestions from various engines and within this dossier
    stack instance and filter/rank them before sending to requestor.

    '''
    if not isinstance(query, unicode):
        query = query.decode('utf8')

    config = yakonfig.get_global_config('memex_dossier.models')
    suggest_services = config.get('suggest_services', [])
    session = requests.Session()
    suggestions = []

    akagraph_config = config.get('akagraph')
    if akagraph_config:
        # (86) 13380344114
        #for candidate in phonenumber_matcher(
        #        query, country='CN'):
        #    query = candidate['canonical']
        #    break
        cluster = []
        logger.info('doing query %r', query)
        cc = akagraph.find_connected_component(query, use_soft=False)
        for rec, confidence in cc:
            rec['confidence'] = confidence
            cluster.append(rec)
        suggestions.append(cluster)

    logger.info('querying %d suggest_services', len(suggest_services))
    for url in suggest_services:
        try:
            url = url % dict(query=query)
        except Exception, exc:
            logger.error('failed to insert query=%r into pattern: %r', query, url)
            continue
        try:
            resp = session.get(url, timeout=5)
        except Exception, exc:
            logger.error('failed to retrieve %r', url)
            continue
        try:
            results = resp.json()
        except Exception, exc:
            logger.error('failed to get JSON from: %r', 
                         resp.content, exc_info=True)
            continue
        if not isinstance(results, list) or len(results) < 2:
            logger.error('got other than list of length at least two from service: %r --> %r',
                         url, results)
            continue
        query_ack = results[0]
        query_suggestions = results[1]
        if not isinstance(query_suggestions, list):
            logger.error('got other than list of query suggestions: %r --> %r',
                         url, results)
            continue
        suggestions += query_suggestions
        logger.info('%d suggestions from %r', len(query_suggestions), url)

    logger.info('found %d suggestions for %r', len(suggestions), query)
    
    cleansed_query = cleanse(query)
    if cleansed_query not in suggestions:
        suggestions.insert(0, query)
    return [query, suggestions] #list(set(suggestions))]


feature_pretty_names = [
    ('ORGANIZATION', 'Organizations'),
    ('PERSON', 'Persons'),
    ('FACILITY', 'Facilities'),
    ('GPE', 'Geo-political Entities'),
    ('LOCATION', 'Locations'),
    ('skype', 'Skype Handles'),
    ('phone', 'Phone Numbers'),
    ('email', 'Email Addresses'),
    ('bowNP_unnorm', 'Noun Phrases'),
    ]


COMPLETED = 'completed'
STORED = 'stored'
HIGHLIGHTS_PENDING = 'pending'
ERROR = 'error'
highlights_kvlayer_tables = {'files': (str, int, str), 'highlights': (str, int, str)}

def make_file_id(file_id_str):
    doc_id, last_modified, content_hash = file_id_str.split('-')
    return doc_id, int(last_modified), content_hash

@app.get('/', json=True)
@app.get('/dossier', json=True)
@app.get('/dossier/', json=True)
@app.get('/dossier/v1', json=True)
@app.get('/dossier/v1/', json=True)
@app.get('/dossier/v1/highlights', json=True)
def wrong_get(response):
    '''
    '''
    response.status = 400
    return 'GET on this endpoint is not defined'

@app.post('/', json=True)
@app.post('/dossier', json=True)
@app.post('/dossier/', json=True)
@app.post('/dossier/v1', json=True)
@app.post('/dossier/v1/', json=True)
def wrong_post(response):
    '''
    '''
    response.status = 400
    return 'POST to this endpoint is not defined'


@app.get('/dossier/v1/highlights/<file_id_str>', json=True)
def v1_highlights_get(response, kvlclient, file_id_str, max_elapsed = 300):
    '''Obtain highlights for a document POSTed previously to this end
    point.  See documentation for v1_highlights_post for further
    details.  If the `state` is still `pending` for more than
    `max_elapsed` after the start of the `WorkUnit`, then this reports
    an error, although the `WorkUnit` may continue in the background.

    '''
    if not file_id_str:
        response.status = 400
        return {'state': ERROR, 'error': {'code': 8, 'message': 'must provide file_id_str'}}
    try:
        file_id = make_file_id(file_id_str)
    except:
        response.status = 400
        return {'state': ERROR, 'error': 
                {'code': 8, 
                 'message': 'unrecognized file_id_str=%r' % file_id_str}}

    kvlclient.setup_namespace(highlights_kvlayer_tables)
    payload_strs = list(kvlclient.get('highlights', file_id))
    if not (payload_strs and payload_strs[0][1]):
        response.status = 500
        payload = {
            'state': ERROR,
            'error': {
                'code': 8,
                'message': 'unknown error'}}
        logger.critical('got bogus info for %r: %r', file_id, payload_strs)
    else:
        payload_str = payload_strs[0][1]
        try:
            payload = json.loads(payload_str)
            if payload['state'] == HIGHLIGHTS_PENDING:
                elapsed = time.time() - payload.get('start', 0)
                if elapsed > max_elapsed:
                    response.status = 500
                    payload = {
                        'state': ERROR,
                        'error': {
                            'code': 8,
                            'message': 'hit timeout'}}
                    logger.critical('hit timeout on %r', file_id)
                    kvlclient.put('highlights', (file_id, json.dumps(payload)))
                else:
                    payload['elapsed'] = elapsed
            logger.info('returning stored payload for %r', file_id)
        except Exception, exc:
            logger.critical('failed to decode out of %r', 
                            payload_str, exc_info=True)
            response.status = 400
            payload = {
                'state': ERROR,
                'error': {
                    'code': 9,
                    'message': 'nothing known about file_id=%r' % file_id}
                }
    # only place where payload is returned
    return payload


@app.post('/dossier/v1/highlights', json=True)
def v1_highlights_post(request, response, kvlclient, tfidf, 
                       min_delay=3, task_master=None):
    '''Obtain highlights for a document POSTed inside a JSON object.

    Get our Diffeo Highlighter browser extension here:
    https://chrome.google.com/webstore/detail/jgfcplgdmjkdepnmbdkmgohaldaiplpo

    While you're at it, pre-register for a beta account on
    http://diffeo.com.

    `min_delay` and `task_master` are used by tests.

    The route for this endpoint is:
    ``POST /dossier/v1/highlights``.

    The expected input structure is a JSON encoded string of an
    object with these keys:

    .. code-block:: javascript
      {
        // only text/html is supported at this time; hopefully PDF.js
        // enables this to support PDF rendering too.
        "content-type": "text/html",

        // URL of the page (after resolving all redirects)
        "content-location": "http://...",

        // If provided by the original host, this will be populated,
        // otherwise it is empty.
        "last-modified": "datetime string or empty string",

        // Boolean indicating whether the content may be stored by the
        // server.  If set to `false`, then server must respond
        // synchronously with a newly computed response payload, and
        // must purge any stored copies of this `content-location`.
        // If `true`, server may respond with `state` of `pending`.
        "store": false,

        // full page contents obtained by Javascript in the browser
        // extension accessing `document.documentElement.innerHTML`.
        // This must be UTF-8 encoded.
        // N.B. This needs experimentation to figure out whether the
        // browser will always encode this as Unicode.
        "body": "... the body content ...",
      }

    The output structure is a JSON UTF-8 encoded string of an
    object with these keys:

    .. code-block:: javascript

      {
        "highlights": [Highlight, Highlight, ...],
        "state":  State,
        "id": StoreID,
        "delay": 10.0,
        "error": Error
      }

    where a `State` is one of these strings: `completed`, `stored`,
    `pending`, or `error`.  The `StoreID` is an opaque string computed
    by the backend that the client can use to poll this end point with
    `GET` requests for a `pending` request.  The `delay` value is a
    number of seconds that the client should wait before beginning
    polling, e.g. ten seconds.

    An `Error` object has this structure:

    .. code-block:: javascript
      {

        // Error codes are (0, wrong content type), (1, empty body),
        // (2, JSON decode error), (3, payload structure incorrect),
        // (4, payload missing required keys), (5, invalid
        // content-location), (6, too small body content), (7,
        // internal error), (8, internal time out), (9, file_id does
        // not exist)
        "code": 0,

        "message": "wrong content_type"
      }

    A `Highlight` object has this structure:

    .. code-block:: javascript

      {
        // float in the range [0, 1]
        "score": 0.7

        // a string presented with a check box inside the options
        // bubble when the user clicks the extension icon to choose
        // which categories of highlights should be displayed.
        "category": "Organization",

        // `queries` are strings that are to be presented as
        // suggestions to the user, and the extension enables the user
        // to click any of the configured search engines to see
        // results for a selected query string.
        "queries": [],

        // zero or more strings to match in the document and highlight
        // with a single color.
        "strings": [],

        // zero or more xpath highlight objects to lookup in the document
        // and highlight with a single color.
        "xranges": [],

        // zero or more Regex objects to compile and
        // execute to find spans to highlight with a single color.
        "regexes": []
      }

    where a Regex object is:

    .. code-block:: javascript

      {
        "regex": "...", // e.g., "[0-9]"
        "flags": "..."  // e.g., "i" for case insensitive
      }

    where an xpath highlight object is:

    .. code-block:: javascript

      {
        "range": XPathRange
      }

    where an XpathRange object is:

    .. code-block:: javascript

      {
        "start": XPathOffset,
        "end": XPathOffset
      }

    where an XpathOffset object is:

    .. code-block:: javascript

      {
        "node": "/html[1]/body[1]/p[1]/text()[2]",
        "idx": 4,
      }

    All of the `strings`, `ranges`, and `regexes` in a `Highlight`
    object should be given the same highlight color.  A `Highlight`
    object can provide values in any of the three `strings`, `ranges`,
    or `regexes` lists, and all should be highlighted.
    '''
    logger.info('handling a POST request to the highlights endpoint')
    tfidf = tfidf or None
    content_type = request.headers.get('content-type', '')
    if not content_type.startswith('application/json'):
        logger.critical('content-type=%r', content_type)
        response.status = 415
        return {
	    'state': ERROR,
            'error': {
                'code': 0,
                'message': 'content_type=%r and should be '
                           'application/json' % content_type,
            },
        }

    body = request.body.read()
    if len(body) == 0:
        response.status = 400
        return {
            'state': ERROR,
            'error': {'code': 1, 'message': 'empty body'}
        }
    try:
        data = json.loads(body.decode('utf-8'))
    except Exception, exc:
        response.status = 400
        return {
	    'state': ERROR,
            'error': {
                'code': 2,
                'message':
                'failed to read JSON body: %s' % exc,
            },
        }

    if not isinstance(data, dict):
        response.status = 400
        return {
	    'state': ERROR,
            'error': {
                'code': 3,
                'message': 'JSON request payload deserialized to'
                      ' other than an object: %r' % type(data),
            },
        }

    expected_keys = set([
        'content-type', 'content-location', 'last-modified', 'body',
	'store',
    ])
    if set(data.keys()) != expected_keys:
        response.status = 400
        return {
	    'state': ERROR,
            'error': {
                'code': 4,
                'message': 'other than expected keys in JSON object. '
                           'Expected %r and received %r'
                           % (sorted(expected_keys), sorted(data.keys())),
            },
        }

    if len(data['content-location']) < 3:
        response.status = 400
        return {
	    'state': ERROR,
            'error': {
                'code': 5,
                'message': 'received invalid content-location=%r'
                           % data['content-location'],
            },
        }

    if len(data['body']) < 3:
        response.status = 400
        return {
	    'state': ERROR,
            'error': {
                'code': 6,
                'message': 'received too little body=%r' % data['body'],
            },
        }

    if data['last-modified']:
        try:
            last_modified = int(datetime.datetime(*eut.parsedate(data['last-modified'])[:6]).strftime('%s'))
        except Exception, exc:
            logger.info('failed to parse last-modified=%r', data['last-modified'])
            last_modified = 0
    else:
        last_modified = 0
    doc_id = md5(data['content-location']).hexdigest()
    content_hash = Nilsimsa(data['body']).hexdigest()
    file_id = (doc_id, last_modified, content_hash)
    file_id_str = '%s-%d-%s' % file_id

    kvlclient.setup_namespace(highlights_kvlayer_tables)
    if data['store'] is False:
        kvlclient.delete('files', (file_id[0],))
        kvlclient.delete('highlights', (file_id[0],))
        logger.info('cleared all store records related to doc_id=%r', file_id[0])
    else: # storing is allowed
        payload_strs = list(kvlclient.get('highlights', file_id))
        if payload_strs and payload_strs[0][1]:
            payload_str = payload_strs[0][1]
            try:
                payload = json.loads(payload_str)
            except Exception, exc:
                logger.critical('failed to decode out of %r', 
                                payload_str, exc_info=True)
            if payload['state'] != ERROR:
                logger.info('returning stored payload for %r', file_id)
                return payload
            else:
                logger.info('previously stored data was an error so trying again')

        delay = len(data['body']) / 5000 # one second per 5KB
        if delay > min_delay:
            # store the data in `files` table
            kvlclient.put('files', (file_id, json.dumps(data)))
            payload = {
                'state': HIGHLIGHTS_PENDING,
                'id': file_id_str,
                'delay': delay,
                'start': time.time()
            }
            # store the payload, so that it gets returned during
            # polling until replaced by the work unit.
            payload_str = json.dumps(payload)
            kvlclient.put('highlights', (file_id, payload_str))

            logger.info('launching highlights async work unit')
            if task_master is None:
                conf = yakonfig.get_global_config('coordinate')
                task_master = coordinate.TaskMaster(conf)
            task_master.add_work_units('highlights', [(file_id_str, {})])

            return payload

    return maybe_store_highlights(file_id, data, tfidf, kvlclient)


def highlights_worker(work_unit):
    '''coordinate worker wrapper around :func:`maybe_create_highlights`
    '''
    if 'config' not in work_unit.spec:
        raise coordinate.exceptions.ProgrammerError(
            'could not run `create_highlights` without global config')

    web_conf = Config()
    unitconf = work_unit.spec['config']
    with yakonfig.defaulted_config([coordinate, kvlayer, dblogger, web_conf],
                                   config=unitconf):
        file_id = make_file_id(work_unit.key)
        web_conf.kvlclient.setup_namespace(highlights_kvlayer_tables)
        payload_strs = list(web_conf.kvlclient.get('files', file_id))
        if payload_strs and payload_strs[0][1]:
            payload_str = payload_strs[0][1]
            try:
                data = json.loads(payload_str)
                # now create the response payload
                maybe_store_highlights(file_id, data, web_conf.tfidf, web_conf.kvlclient)
            except Exception, exc:
                logger.critical('failed to decode data out of %r', 
                                payload_str, exc_info=True)
                payload = {
                    'state': ERROR,
                    'error': {
                        'code': 7,
                        'message': 'failed to generate stored results:\n%s' % \
                        traceback.format_exc(exc)}
                    }
                payload_str = json.dumps(payload)
                kvlclient.put('highlights', (file_id, payload_str))
                

def maybe_store_highlights(file_id, data, tfidf, kvlclient):
    '''wrapper around :func:`create_highlights` that stores the response
    payload in the `kvlayer` table called `highlights` as a stored
    value if data['store'] is `False`.  This allows error values as
    well as successful responses from :func:`create_highlights` to
    both get stored.

    '''
    payload = create_highlights(data, tfidf)
    if data['store'] is True:
        stored_payload = {}
        stored_payload.update(payload)
        stored_payload['state'] = STORED
        payload_str = json.dumps(stored_payload)
        kvlclient.put('highlights', (file_id, payload_str))
    return payload


def create_highlights(data, tfidf):
    '''compute highlights for `data`, store it in the store using
    `kvlclient`, and return a `highlights` response payload.

    '''
    try:
        fc = etl.create_fc_from_html(
            data['content-location'], data['body'], tfidf=tfidf, encoding=None)
    except Exception, exc:
        logger.critical('failed to build FC', exc_info=True)
        return {
            'state': ERROR,
            'error': {'code': 7,
                      'message': 'internal error: %s' % traceback.format_exc(exc),
                      }
        }
    if fc is None:
        logger.critical('failed to get FC using %d bytes from %r',
                        len(data['body']), data['content-location'])
        response.status = 500
        return {
            'state': ERROR,
            'error': {
                'code': 7,
                'message': 'internal error: FC not generated for that content',
            },
        }
    try:
        highlights = dict()
        for feature_name, pretty_name in feature_pretty_names:
            # Each type of string is
            if feature_name not in fc:
                continue
            total = sum(fc[feature_name].values())
            bow = sorted(fc[feature_name].items(), key=itemgetter(1), reverse=True)
            highlights[pretty_name] = [(phrase, count / total)
                                       for phrase, count in bow]
            logger.info('%r and %d keys',
                        feature_name, len(highlights[pretty_name]))

        highlight_objs = build_highlight_objects(data['body'], highlights)
    except Exception, exc:
        logger.critical('failed to build highlights', exc_info=True)
        return {
            'state': ERROR,
            'error': {'code': 7,
                      'message': 'internal error: %s' % traceback.format_exc(exc),
                      }
        }

    payload = {
        'highlights': highlight_objs,
        'state': COMPLETED,
    }
    return payload


def build_highlight_objects(html, highlights, uniformize_html=True):
    '''converts a dict of pretty_name --> [tuple(string, score), ...] to
    `Highlight` objects as specified above.

    '''
    if uniformize_html:
        try:
            html = uniform_html(html.encode('utf-8')).decode('utf-8')
        except Exception, exc:
            logger.info('failed to get uniform_html(%d bytes) --> %s',
                        len(html), exc, exc_info=True)
            html = None

    highlight_objects = []
    for category, phrase_scores in highlights.iteritems():
        for (phrase, score) in phrase_scores:
            hl = dict(
                score=score,
                category=category,
                )
            ranges = make_xpath_ranges(html, phrase)
            if ranges:
                hl['xranges'] = [{'range': r, 'phrase': phrase} for r in ranges]
            elif phrase in html:
                hl['strings'] = [phrase]
            else:
                hl['regexes'] = [{
                    'regex': phrase,
                    'flags': 'i',
                }]
            highlight_objects.append(hl)
    return highlight_objects


def make_xpath_ranges(html, phrase):
    '''Given a HTML string and a `phrase`, build a regex to find offsets
    for the phrase, and then build a list of `XPathRange` objects for
    it.  If this fails, return empty list.

    '''
    if not html:
        return []
    if not isinstance(phrase, unicode):
        try:
            phrase = phrase.decode('utf8')
        except:
            logger.info('failed %r.decode("utf8")', exc_info=True)
            return []
    phrase_re = re.compile(
        phrase, flags=re.UNICODE | re.IGNORECASE | re.MULTILINE)
    spans = []
    for match in phrase_re.finditer(html, overlapped=False):
        spans.append(match.span())  # a list of tuple(start, end) char indexes

    # now run fancy aligner magic to get xpath info and format them as
    # XPathRange per above
    try:
        xpath_ranges = list(char_offsets_to_xpaths(html, spans))
    except:
        logger.info('failed to get xpaths', exc_info=True)
        return []
    ranges = []
    for xpath_range in filter(None, xpath_ranges):
        ranges.append(dict(
            start=dict(node=xpath_range.start_xpath,
                       idx=xpath_range.start_offset + 1),
            end=dict(node=xpath_range.end_xpath,
                     idx=xpath_range.end_offset)))

    return ranges


known_engines = set(['google', 'dark', 'cdr', 'twitter_cluster', 'company_profile'])

@app.post('/dossier/v1/recommend')
def v1_recommend(request, response, config):
    try:
        data = json.load(request.body)
    except Exception as exc:
        return json.dumps({'error': 'invalid json: %s' % traceback.format_exc(exc)})
    limit = data.get('limit', 10)
    engine_name = data.get('engine', 'google')
    if 'fc' not in data:
        logger.warn(json.dumps(data, indent=4, sort_keys=True))
        return json.dumps({'error': '"fc" missing!'})

    fc = FeatureCollection.from_dict(data['fc'])
    if engine_name not in known_engines:
        return json.dumps({'error': '%r not in known engines: %r'
                           % (engine, known_engines)})

    recommendations = get_recommendations(fc, config, limit, engine_name)
    response.status = 200
    response.content_type = 'application/json'
    return json.dumps(recommendations, indent=4, sort_keys=True)
