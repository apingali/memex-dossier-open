'''AKAGraph core components

.. This software is released under an MIT/X11 open source license.
   Copyright 2015-2016 Diffeo, Inc.
'''
from __future__ import absolute_import, division, print_function
import string
import argparse
from collections import Counter, defaultdict
import gzip
import mmh3
from itertools import islice
import json
import logging
from operator import itemgetter
import sys
import time
import math
import csv
import random

import kvlayer
import yakonfig
import dblogger

import cbor

from elasticsearch import Elasticsearch, RequestError, NotFoundError
from elasticsearch.helpers import bulk, scan, ScanError
from collections import OrderedDict

from memex_dossier.handles.soft_selector_score import \
    prob_username, load_ngrams
from .etl import get_etl_transforms

logger = logging.getLogger(__name__)

RECORD_TYPE = 'record'
UNION_FIND_TYPE = 'union_find'
ROOT_SIZE_TYPE = 'root_size'

default_soft_selectors = ['name', 'username', 'postal_address']
default_hard_selectors = ['email', 'phone', 'skype', 'hostname']


def lru_cache(cache_size):
    def lru_decorator(f):
        od = OrderedDict()
        size = cache_size

        def newfun(*args):
            key = (tuple(args))
            print(key)
            if key in od:
                val = od.pop(key)
            else:
                val = f(*args)
            od[key] = val
            if len(od) > size:
                od.popitem(last=False)
            return val

        return newfun
    return lru_decorator


def pseudorandom(*args):
    raw = (1 << 31) + mmh3.hash(json.dumps(args))
    return float(raw) / (1 << 32)

def uniform_random():
    """ this function exists as a level of redirection so that the tests 
    can overwrite it to get deterministic behavior
    """
    return random.uniform(0, 1)

class AKANode(object):

    def __init__(self, name, replica):
        self.name = name
        self.replica = replica
        # these are set whenever a node is looked up.
        self.rank = None
        self.cardinality = None

    def set_rank_from_record(self, record=None):
        if record:
            assert('rank' in record and 'cardinality' in record)
            self.rank = record['rank']
            self.cardinality = record['cardinality']
        else:
            self.rank = 1
            self.cardinality = 1

    def __hash__(self):
        return hash((self.name, self.replica))

    def __eq__(self, other):
        return self.get_id() == other.get_id()

    def get_id(self):
        ret = self.name
        if self.replica is not None:
            ret = str(self.replica) + '://' + ret
        return ret

    def to_record(self):
        assert self.replica is not None
        return [self.name, self.get_id()]

    @classmethod
    def from_record(cls, record):
        assert(record[0] in record[1])
        name = record[0]
        replica = record[1].split(':')[0]
        return AKANode(name, replica)


class MemoryUnionFind():
    def __init__(self):
        self.parents = {}
        self.ranks = defaultdict(lambda: 1)

    def _find(self, name):
        seen = set()
        while name in self.parents:
            seen.add(name)
            name = self.parents[name]
        # name is now the root
        for s in seen:
            self.parents[s] = name
        return name

    def find_all_and_union(self, *names):
        if len(names) < 2:
            return []
        roots = {self._find(name) for name in names}
        if len(roots) < 2:
            return []
        ranked_roots = sorted([(self.ranks[root], root) for root in roots])
        new_rank, new_root = ranked_roots.pop()
        if ranked_roots[-1][0] == new_rank:
            new_rank += 1
        self.ranks[new_root] = new_rank
        for _, old_root in ranked_roots:
            self.parents[old_root] = new_root
        return roots


class AKAGraph(object):
    def __init__(self, hosts=None, index_name=None, replicas=10,
                 soft_selectors=None, hard_selectors=None,
                 hyper_edge_scorer=None,
                 shards=None, buffer_size=20, conn=None,
                 num_identifier_downweight=0,
                 popular_identifier_downweight=0,
                 ):
        '''AKAGraph provides the interface to an elastic-search backed
        probabilistic graph proximity engine

        Its main operations are:
         * add a "record" containing various types of identifiers
         * query for those records "close" to a given identifier

        :param hosts: elasticsearch hosts

        :param index_name: the elasticsearch index name to use (or create)

        :param replicas: the number of monte-carlo samples to use

        :param soft_selectors: a list of identifiers to be considered
        not globally unique

        :param hard_selectors: a list of globally unique identifiers

        :param shards: number of elasticsearch shards

        :param buffer_size: how many updates to batch before
        committing them to elasticsearch

        :param conn: Elasticsearch connection object

        :param num_identifiers_downweight: records with many
        identifiers should have their identifiers bind more loosely to
        others

        :param popular_identifier_downweight: identifiers in many
        records should bind loosely

        '''

        if conn is None:
            self.conn = Elasticsearch(hosts=hosts, retry_on_timeout=True,
                                      max_retries=5)
        else:
            self.conn = conn
        self.index = index_name
        self.shards = shards
        self.buffer_size = buffer_size
        self.record_buffer = []
        self.edge_buffer = []
        self.in_context = False
        if soft_selectors is None:
            soft_selectors = default_soft_selectors
        self.soft_selectors = set(soft_selectors)
        if hard_selectors is None:
            hard_selectors = default_hard_selectors
        self.hard_selectors = set(hard_selectors)
        if hyper_edge_scorer is not None:
            self.hyper_edge_scorer = hyper_edge_scorer
        else:
            unigrams, bigrams = load_ngrams()
            self.hyper_edge_scorer = \
                lambda s: prob_username(s, unigrams, bigrams)
        self.replica_list = range(replicas)
        self.score_cutoff = .001
        self.num_identifier_downweight = num_identifier_downweight
        self.popular_identifier_downweight = popular_identifier_downweight

    def __enter__(self):
        logger.debug('in context')
        self.in_context = True
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.in_context = False
        if exc_type is None and exc_value is None and traceback is None:
            self.flush()

    def add(self, rec, analyze_and_union=True):
        '''add `rec` to ES;  must be used inside a `with` statement
        '''
        assert self.in_context, 'must use "with" statement to add docs'
        self.record_buffer.append((rec, analyze_and_union))
        if len(self.record_buffer) >= self.buffer_size:
            self.flush_records()

    def add_edge(self, IDs, strength, evidence=None):
        '''
        Adds an edge between all identifiers in the iterable IDs with the given strength.
        This does not create entries of type RECORD.  
        It will simply do unions in the UNION_FIND type.
        However, if you have records ingested, and these id's correspond to the urls of those records, 
        ingesting this way will link these records just as when full record-based ingest happens.
        If evidence is None, each call to add_edge should be thought of as offering independent evidence of a relationship.
        So if you call add_edge(["A", "B"], .5) twice, this is equivalent to calling it once with strength .75
        However add_edge(["A", "B"], .5, 'foo') is idempotent 

        add_edge must be used inside a `with` statement

        :param IDs: An iterable of identifiers to union probabilistically.  
                    These can be any string, but if the match urls of records, then querying based on those record's fields will work as expected.
        :param strength: 0 < strength <= 1 is a probability with which to union all edges in the IDs set
        :param evidence: evidence is used for fine-grained control over whether repeated calls with overlapping IDs sets are treated independently.
                         For example, If you like A and B because they share a username "foo", and you like B and A because they share "foo", 
                         you want it to only link once.  Supply "foo" as evidence and this will work as desired.  
                         If you later want to link A and B because they share an email address, this is independent evidence and will increase the proximity of A and B
        '''
        
        assert self.in_context, 'must use "with" statement to add docs'
        self.edge_buffer.append((IDs, strength, evidence))
        if len(self.edge_buffer) >= self.buffer_size:
            self.flush_edges()

    def flush(self):
        self.flush_records()
        self.flush_edges()

    def flush_edges(self):
        local_union_find = MemoryUnionFind()  # this is purely an efficiency hack so we hit ES less redundantly
        for equivs, score, score_reason in self.edge_buffer:
            logger.debug('given equivs %r with %s strength and evidence %s',
                         equivs, score, score_reason)
            self.probabilistically_unite_edges(equivs, score, score_reason, local_union_find)
        self.edge_buffer = self.edge_buffer[:0]

    def flush_records(self):
        '''Actually do the work to ingest records gathered by calls to `add`.
        All vertexes are their own roots on initial ingest; so this
        sets size to 1 iff the doc has not been ingested before.

        '''
        if not self.conn.indices.exists(index=self.index):
            self.create_index()
        logger.debug('flushing ingest buffer (size: %d)', len(self.record_buffer))
        actions = []
        for rec, _ in self.record_buffer:
            actions.append({
                '_index': self.index,
                '_type': RECORD_TYPE,
                '_id': rec['url'],
                '_source': rec,
            })
            #actions.append({
            #    '_index': self.index,
            #    '_type': ROOT_SIZE_TYPE,
            #    '_id': rec['url'],
            #    '_op_type': 'update',
            #    'doc': {'size': 1}, # set initial size to 1, TODO:
                                    # write tests to make sure this
                                    # doesn't change values when
                                    # re-ingesting
            #    'doc_as_upsert': True,
            #})
        bulk(self.conn, actions, timeout='60s')
        # next find equivalent records via exact match, and union them

        # uh oh...
        self.sync()

        # as an efficiency hack we make a local, one-off union find so we hit ES less redundantly
        # batches are likely to have a lot of the same records to union, and we do not want
        # to tell ES about each of a set of redundant unions.  If we catch them locally, we only
        # hit ES with new stuff
        local_union_find = MemoryUnionFind()  

        # record_buffer has tuples where the [0] element is the record 
        # and the [1] element is whether or not to union from it.  Only process the records to union here
        # this supports adding records and *explicit* edges separately
        for rec, score, score_reason, equivs in self.find_equivs([buf[0] for buf in self.record_buffer if buf[1]]):
            logger.debug('%s found %d (%f) equivs for %r --> %r',
                         score_reason, len(equivs), score, rec['url'], equivs)
            equivs.add(rec['url'])
            self.probabilistically_unite_edges(equivs, score, score_reason, local_union_find)
        self.record_buffer = self.record_buffer[:0]

    def probabilistically_unite_edges(self, equivs, score, score_reason, local_union_find=None):
        if score == 1:
            equivs_len = len(equivs)
            if local_union_find:
                equivs = local_union_find.find_all_and_union(*equivs)
                logger.debug('had %s equivs, now have %s',
                             equivs_len, len(equivs))
            if len(equivs) < 1:
                return
        if score_reason:
            def include_replica(replica):
                return score == 1 or pseudorandom(score_reason, replica) < score
        else:
            # if no reason is given, make it random
            def include_replica(replica):
                del replica
                return score == 1 or uniform_random() < score

        for replica in self.replica_list:
            if include_replica(replica):
                self.unite(*[AKANode(url, replica) for url in equivs])
        self.sync()


    def sync(self):
        '''Forces data to disk, so that data from all calls to `put` will be
        available for getting and querying.  Generally, this should
        only be used in tests.

        '''
        self.conn.indices.refresh(index=self.index)

    def analyze_clusters(self, limit=None):
        '''hunt for clusters and return a list of clusters sored by size and
        indication of their overlaps:

        .. block-quote:: python
            [(size, [rec1, rec2, ...], {phone: ['+.....']}, ...]

        '''
        #i_recs = islice(loader(path, hard_selectors=self.hard_selectors), limit)            
        clusters = []
        # consider only clusters of at least two records
        for root_url, count in self.get_all_roots(size_limit=1, 
                                                  candidates_limit=limit):
            del count
            cc = list(self.connected_component(root_url))
            # The sequence of steps up to this point scans all
            # records, gathers their roots with counts of how many
            # records are under that root, then gets the CC for the
            # root... which should be the exact same set, right?  This
            # may be the source of the big clusters in DIFFEO-2305
            #assert len(cc) == count, (count, len(cc), cc)
            logger.debug('found connected component of %d: %r', len(cc), cc)
            recs = list(self.get_recs(*cc))
            overlaps = find_overlaps(recs)
            _recs = {}
            for rec in recs:
                _recs[rec['url']] = rec
            clusters.append({"count": len(cc), "records": _recs, "overlaps": overlaps})
        clusters.sort(key=itemgetter('count'), reverse=True)
        cluster_sizes = Counter()
        for size, _, _ in clusters:
            cluster_sizes[size] += 1
        data = {
            'clusters': clusters,
            'aggregate_stats': {
                'largest': clusters[0]['count'],
                'median': clusters[len(clusters) // 2]['count'],
                'mean': sum([cluster['count'] for cluster in clusters]) / len(clusters),
                'smallest': clusters[-1]['count'],
                'histogram': dict(cluster_sizes),
            }
        }
        return data

    def find_equivs(self, records):
        '''For an iterable of `records`, yield tuples of `(record, score,
        equivs)`, where a `record` from `records` might appear in
        multiple of the yielded.

        '''
        queries = []
        scores = []
        rec_pointers = [] # carries a pointer to a record for each query
        for rec in records:
            # compute score multiplies for this record
            weight = 1.0
            if self.num_identifier_downweight:
                count = sum([len(values) for key,values in rec.iteritems() if (key in self.hard_selectors or key in self.soft_selectors)])
                weight = math.exp(-self.num_identifier_downweight * (count-1))
                logger.debug('weight = %f, %d, %s', weight, count, rec['url'])

            # first we gather one query for all hard selectors
            hard_or_query = []
            for key, values in rec.iteritems():
                if key in self.hard_selectors:
                    for v in values:
                        hard_or_query.append({'term': {key: v}})
            if hard_or_query:
                query = {
                    "query": {
                        "constant_score": {
                            "filter": {
                                "bool": {
                                    "should": hard_or_query,
                                    "must_not": {"ids": {"values": [rec["url"]]}},
                                }
                            }
                        }
                    }
                }
                queries.append({'index': self.index, 'type': RECORD_TYPE, '_source_include': []})
                queries.append(query)
                scores.append((weight, json.dumps(hard_or_query)))
                rec_pointers.append(rec)
            else:
                logger.debug('skipping because no hard identifiers')
            # next, we make separate queries for each soft selector
            if not self.hyper_edge_scorer or len(self.replica_list) == 1:
                continue
            for key, values in rec.iteritems():
                if key not in self.soft_selectors: continue
                for v in values:
                    if not v:
                        continue
                    query = {
                        "query": {
                            "constant_score": {
                                "filter": {
                                    "bool": {
                                        "should": [{'term': {key: v}}],
                                        "must_not": {"ids": {"values": [rec["url"]]}},
                                    }
                                }
                            }
                        }
                    }
                    score = self.hyper_edge_scorer(v)
                    if score > self.score_cutoff:
                        logger.debug('soft selector score %.3f for %r', score, v)
                        queries.append({'index': self.index, 'type': RECORD_TYPE, '_source_include': []})
                        queries.append(query)
                        scores.append((score * weight, v))
                        rec_pointers.append(rec)

        # helper function for stripping down to just the URL
        def hits_generator(hits):
            for hit in hits['hits']['hits']:
                yield hit['_id']
        # now loop until we get answers for all the queries
        cursor = 0
        while queries:
            res = self.conn.msearch(body=queries)
            for hits in res['responses']:
                # remove the corresponding two rows of queries and corresponding record
                queries.pop(0); queries.pop(0)
                record = rec_pointers[cursor]
                score, score_reason = scores[cursor]

                # revise_score
                cursor += 1
                if 'error' in hits:
                    # need to run msearch again, starting with the query after the failed one
                    if 'queue capacity' not in hits['error']:
                        logger.warn("Error getting equivs for %s: %s", record, hits['error'])
                    break
                else:
                    hits_set = set(hits_generator(hits))
                    if hits_set:
                        if self.score_cutoff < score < 1:
                            logger.debug("SOFT: %d, %s", score, score_reason)
                        if self.popular_identifier_downweight:
                            score = score * math.exp(- self.popular_identifier_downweight * (len(hits_set)-1)) 

                        yield (record, score, score_reason, hits_set)

    def get_recs(self, *urls):
        '''get records one or more for `urls`
        '''
        if not urls:
            raise Exception('called get_recs with empty list')
        resp = self.conn.mget(
            index=self.index, doc_type=RECORD_TYPE,
            body = {'ids': urls})
        for rec in resp['docs']:
            if not rec['found']:
                yield {"url": rec['_id']}
                #raise KeyError('missing: %r' % rec['_id'])
            else:
                yield rec['_source']

    def get_all_urls(self, limit=None):
        '''get all urls in the index
        '''
        res = scan(
            self.conn, index=self.index, doc_type=RECORD_TYPE,
            _source_include=[],
            query={'query': {'match_all': {}}})
        for item in islice(res, limit):
            yield item['_id']

    def find_urls_by_selector(self, selector, use_soft=True):
        if not self.conn.indices.exists(index=self.index):
            self.create_index()
        or_query = [{'term': {'url': selector}}]
        for key in self.hard_selectors:
            or_query.append({'term': {key: selector}})
        if use_soft:
            for key in self.soft_selectors:
                or_query.append({'term': {key: selector}})
            logger.debug('including soft_selectors: %r', self.soft_selectors)
        query = {
            "query": {
                "bool": {
                    "should": or_query,
                }
            }
        }
        # logger.debug(json.dumps(query, indent=4, sort_keys=True))
        try:
            res = self.conn.search(
                index=self.index, doc_type=RECORD_TYPE,
                _source_include=[], body=query)
            '''
            body={
                'query': {
                    'multi_match': {
                        'query': selector,
                        'type': 'cross_fields',
                        # TODO: blend soft_selectors into this
                        'fields': self.hard_selectors,
                        }
                    }
                })
            '''
            visited_urls = set()
            for hit in res['hits']['hits']:
                # logger.debug(hit['_score'])
                url = hit['_id']
                if url not in visited_urls:
                    visited_urls.add(url)
                    yield url
        except NotFoundError, exc:
            logger.warn('akagraph indexes do not exist yet: %s', exc)
            return

    def find_connected_component(self, selector, use_soft=True):
        urls = set(self.find_urls_by_selector(selector, use_soft))
        # logger.debug('get %d equivs for %r', len(equivs), selector)
        if not urls:
            urls.add(selector)
        ccs = list(self.connected_component(*urls))
        if len(ccs) == 1:
            # degenerate case where only this record (potentially empty) was found
            # the only url found is the selector argument, see if it has a real record
            rec = list(self.get_recs(selector))[0]
            if len(rec) > 1:  # only yield this record if it is non-empty
                yield rec, 1.0
        else:
            for url, count in ccs:
                rec = list(self.get_recs(url))[0]
                yield rec, count / len(self.replica_list)

    def get_children(self, node):
        '''get child URLs of `url`
        '''
        assert node.replica is not None
        res = scan(
            self.conn, index=self.index, doc_type=UNION_FIND_TYPE,
            _source_include=[],
            query={'query': {'term': {'parent': node.get_id()}}})
        for item in res:
            yield AKANode.from_record(item['_source']['child'])

    def get_parent(self, node):
        '''get parent URL of `url`
        require node to have a replica id 
        returns None if this node is a root
        '''
        max_tries = 3
        tries = 0
        assert node.replica is not None
        while tries < max_tries:
            tries += 1
            res = self.conn.search(
                index=self.index, doc_type=UNION_FIND_TYPE,
                _source_include=['parent', 'rank', 'cardinality'],
                #raise_on_error=False,
                size=len(self.replica_list),
                body={'query': {'term': {'child': node.get_id()}}})
            try:
                hits = res['hits'].get('hits', [])
                if hits:
                    record = hits[0]['_source']
                    if 'parent' in record:
                        return AKANode.from_record(record['parent'])
                    else:
                        node.set_rank_from_record(record)
                else:
                    node.set_rank_from_record(None)
                return None
            except ScanError, exc:
                logger.critical('trapping and retrying %d more times: %s',
                                max_tries - tries, str(exc))
                continue
        # if we got here, it means we got errors every time

    def get_all_unions(self):
        '''
        '''
        res = scan(
            self.conn, index=self.index, doc_type=UNION_FIND_TYPE)
        for item in res:
            yield item['_source']

    def get_sizes(self, *ids):
        '''Query the `root_size` doc type for a list of `ids` and return a
        dict mapping `_id` to its size.

        '''
        resp = self.conn.mget(
            index=self.index, doc_type=ROOT_SIZE_TYPE,
            body = {'ids': ids})
        sizes = {}
        for rec in resp['docs']:
            if not rec['found']:
                logger.critical('id=%r not in root_size', rec['_id'])
                continue
            sizes[rec['_id']] = rec['_source']['size']
        return sizes

    def set_parents(self, new_root, *others):
        '''set a root for a set of other nodes URLs for `child` URLs by assembling a
        batch from `pairs`=[(`child`, `parent`), ...]

        '''
        actions = [{
            '_index': self.index,
            '_type': UNION_FIND_TYPE,
            '_id': new_root.get_id(),
            '_op_type': 'index',
            '_source': {
                'child': new_root.to_record(),
                'replica': new_root.replica,
                'rank': new_root.rank,
                'cardinality': new_root.cardinality,
            },
        }]
        for child in others:
            assert child.name != new_root.name
            actions.append({
                '_index': self.index,
                '_type': UNION_FIND_TYPE,
                '_id': child.get_id(),
                '_op_type': 'index',
                '_source': {
                    'parent': new_root.to_record(),
                    'child': child.to_record(),
                    'replica': child.replica
                },
            })
        logger.debug('set_parent bulk actions: %r', actions)
        bulk(self.conn, actions, timeout='60s')
        #print(actions)


    def get_all_roots(self, size_limit=0, candidates_limit=None, replica=0):
        '''yield all of the roots with more than `size_limit` children.
        Default `size_limit` is zero, which yields all roots.

        '''
        roots = Counter()
        for url in self.get_all_urls(limit=candidates_limit):
            root = self.get_root(AKANode(url, replica))
            roots[root.name] += 1
        for root, count in roots.most_common():
            if count > size_limit:
                yield root, count


    def get_root(self, node):
        '''Find the root URL for `url`, which is `url` itself if it has not
        been united with anything.  A root `url` has itself as root.

        '''
        seen = set()
        while True:
            parent = self.get_parent(node)
            if not parent: break
            if parent.name in seen:
                logger.critical('hit loop: %r', seen)
                sys.exit()
            seen.add(parent.name)
            node = parent
        assert(node.cardinality)
        # TODO: turn get_root, into get_roots
        # TODO: implement path compression
        return node

    def unite(self, *nodes):
        roots = {root.name: root for root in [self.get_root(node) for node in nodes]}
        roots = sorted(roots.values(), key=lambda n: (n.rank, pseudorandom(n.name, n.replica)))
        if len(roots) == 1:
            logger.debug('already united')
            return
        
        new_root = roots.pop()
        if new_root.rank == roots[-1].rank:
            new_root.rank += 1
        for root in roots:
            new_root.cardinality += root.cardinality
            #logger.debug('%d pairs built for union', len(roots))
        self.set_parents(new_root, *roots)
        return new_root

    def connected_component(self, *urls):
        frontier = set()
        for url in urls:
            for replica in self.replica_list:
                frontier.add(self.get_root(AKANode(url, replica)))
        frontier = list(frontier)
        counts = defaultdict(lambda: 0)
        for node in frontier:
            counts[node.name] += 1
            frontier.extend(self.get_children(node))
            # compute a cutoff?
    # sort all of the nodes in connected components so the highest count urls are first
        sorted_list = sorted(counts.items(), key=(lambda t: (-t[1], t[0])))
        # compute a cutoff?
        # arbitrary cutoff is: yielded at least 10 and count <= 2
        so_far = 0
        for url, count in sorted_list:
            so_far += 1
            if so_far > 10 and count <= 2:
                break
            yield url, count

    def delete_index(self):
        try:
            self.conn.indices.delete(index=self.index)
        except NotFoundError:
            pass

    def create_index(self):
        try:
            settings = {}
            # Number of shards can never be changed after creation time!
            if self.shards is not None:
                settings['number_of_shards'] = self.shards
            self.conn.indices.create(index=self.index, body={
                'settings': settings,
            })
        except RequestError:
            # Already exists.
            return

        properties = {'url': {
            'type': 'string',
            'index': 'not_analyzed',
        }}

        for soft_selector in self.soft_selectors:
            properties[soft_selector] = {
                'type': 'string',
                'index': 'not_analyzed',
            }
        for hard_selector in self.hard_selectors:
            properties[hard_selector] = {
                'type': 'string',
                'index': 'not_analyzed',
            }

        self.conn.indices.put_mapping(
            index=self.index, doc_type=RECORD_TYPE, body={
                RECORD_TYPE: {
                    '_all': {
                        'enabled': False,
                    },
                    'properties': properties
                },
            })

        self.conn.indices.put_mapping(
            index=self.index, doc_type=UNION_FIND_TYPE, body={
                UNION_FIND_TYPE: {
                    
                    '_all': {
                        'enabled': False,
                    },
                    # _id is *child* vertex's identifier with replica, i.e. `url`
                    # each child has either a parent OR is a root and has a rank and cardinality
                    'properties': {
                       "parent": {
                            "type": "string", 
                            "index": "not_analyzed",
                        },
                        "child": {
                            "type": "string",
                            "index": "not_analyzed",
                        },
                        "replica": {
                            "type": "string",
                            "index": "not_analyzed",
                        },
                        "rank": {
                            "type": "integer",
                         },
                        "cardinality": {
                            "type": "integer",
                        },
                    },
                },
            })

        self.conn.indices.put_mapping(
            index=self.index, doc_type=ROOT_SIZE_TYPE, body={
                ROOT_SIZE_TYPE: {
                    '_all': {
                        'enabled': False,
                    },
                    # Every vertex is in this table.  _id is *root*
                    # vertex's identifier, i.e. `url`. If its value is
                    # 0, then it is not a root.  This enables ingest
                    # to be indempotent, because it allows ingest to
                    # detect that a record has already been ingested.
                    # The size of a root that has only itself as a
                    # child is one.
                    'properties': {
                        'size': {
                            'type': 'integer',
                        },
                    },
                },
            })


def find_overlaps(recs):
    '''Find all of the overlapping identifiers in a list of records and
    return them as a map<identifier_type, list<identifier>>

    '''
    counter = Counter()
    for rec in recs:
        for itype, fiers in rec.iteritems():
            if not isinstance(fiers, list): continue
            for ident in fiers:
                counter[(itype, ident)] += 1

    _overlaps = defaultdict(dict)
    for (itype, ident), count in counter.most_common():
        if count == 1: continue
        _overlaps[itype][ident] = count

    overlaps = {}
    for itype, fiers in _overlaps.iteritems():
        overlaps[itype] = dict(fiers)
    return overlaps


def main():
    from memex_dossier.models.web.config import Config

    p = argparse.ArgumentParser('Ingest AKA records into ElasticSearch.')
    p.add_argument('--k-replicas', default=1, type=int)
    p.add_argument('--buffer-size', default=100, type=int)
    p.add_argument('--delete', action='store_true', default=False)
    p.add_argument('--parent')
    p.add_argument('--query')
    p.add_argument('--make-pairs', help='path to a csv file to create')
    p.add_argument('--input-format', default=None)
    p.add_argument('--ingest', nargs='+',
                   help='record files in gzipped CBOR or an ETL format.')
    p.add_argument('--analyze', action='store_true', default=False,
                   help='output analysis of all clusters')
    p.add_argument('--limit', default=None, type=int,
                   help='number of records to process.')
    config = Config()
    args = yakonfig.parse_args(p, [dblogger, config, kvlayer, yakonfig])

    logging.basicConfig(level=logging.DEBUG)

    aka = config.akagraph
    aka.buffer_size = args.buffer_size

    if args.parent:
        data = [aka.get_parent(AKANode(unicode(args.parent), i))
                for i in aka.replica_list]
        logger.info(data)
        sys.exit()

    if args.query:
        cluster = []
        ccs = aka.find_connected_component(args.query)
        for rec, confidence in ccs:
            rec['confidence'] = confidence
            cluster.append(rec)
        if not args.make_pairs:
            logger.info(json.dumps(cluster, indent=4, sort_keys=True))
        else:
            assert args.make_pairs.endswith('.csv'),\
                '--make-pairs must end in ".csv"'
            # make the four-column Memex eval format
            # these functions are used in the loop below

            def make_name(rec):
                vals = []
                for key in ['email', 'name', 'phone', 'bitcoin']:
                    if key not in rec: continue
                    vals.extend(rec[key])
                return u','.join(vals)

            def domain(rec):
                parts = rec['url'].split('/')
                if len(parts) < 3:
                    logger.debug(parts)
                    return parts[0]
                return parts[2]

            def tld(rec):
                return domain(rec).split('.')[-1]

            def bad_username(un):
                if un[0] in set(string.digits): return True
                for bad_word in ['Home', 'Wish', 'Cart', 'Shopping', 'Account', 'User']:
                    if bad_word in un: return True
                if len(set(un) - set(string.digits + string.letters)) > 0 : return True
                return False
            with open(args.make_pairs, 'ab') as fh:
                writer = csv.writer(fh)
                # consider all pairs
                for i in range(len(cluster)):
                    for j in range(i, len(cluster)):
                        r1 = cluster[i]
                        r2 = cluster[j]
                        if 'bogus' in r1['url']: continue
                        if 'bogus' in r2['url']: continue
                        c1 = r1['confidence']
                        c2 = r2['confidence']
                        if not (1 <= c1 or 1 <= c2):
                            continue
                        score = min(r1['confidence'], r2['confidence'])
                        pair_key = ','.join([args.query, domain(r1), domain(r2)])
                        tld_key = ','.join(sorted([tld(r1), tld(r2)]))
                        identifier_types = ['email', 'username']  #### IGNORE phone and bitconin and name
                        for k1 in identifier_types:
                            if k1 not in r1: continue
                            for k2 in identifier_types:
                                if k2 not in r2: continue
                                identifier_type_key = ','.join(sorted([k1, k2]))
                                for n1 in r1[k1]:
                                    for n2 in r2[k2]:
                                        if k1 == 'email':
                                            n1 = n1.split('@')[0]
                                        if k2 == 'email':
                                            n2 = n2.split('@')[0]  ### they only want to *see* a username-like string
                                        if bad_username(n1): continue
                                        if bad_username(n2): continue
                                        row = (
                                            r1['url'],
                                            n1.encode('utf8'),
                                            r2['url'],
                                            n2.encode('utf8'),
                                            score,
                                            identifier_type_key,
                                            pair_key,
                                            tld_key,
                                        )
                                        writer.writerow(row)
        sys.exit()

    if args.delete:
        aka.delete_index()
        sys.exit()

    if args.input_format:
        loader = get_etl_transforms(args.input_format)
    else:
        loader = None

    if args.ingest:
        logger.debug('running ingest with loader=%r: %r',
                     loader, aka)
        run_ingest(args, loader, aka)

    if args.analyze:
        stats = aka.analyze_clusters(limit=args.limit)
        print(json.dumps(stats, indent=4, sort_keys=True))
        sys.exit()


def run_ingest(args, loader, aka):
    '''calls on `aka.add` on each record from `loader` acting on each path
    in `args.ingest`

    '''
    total = 0
    start = time.time()
    with aka:
        for rec_path in args.ingest:
            logger.debug('loading %r', rec_path)
            if loader:
                temp_loader = loader
            else:
                def temp_loader(path, **kwargs):
                    del kwargs
                    fopen = open
                    loads = cbor.load
                    if path.endswith('.gz'):
                        fopen = gzip.open
                    elif path.endswith('.json'):
                        def loads(f):
                            while True:
                                line = fdoc.readline()
                                if not line:
                                    raise EOFError()
                                try:
                                    return json.loads(line)
                                except ValueError:
                                    logger.debug('failed to decode json %s', line)
                    elif path == '-':
                        fopen = lambda s: sys.stdin
                    with fopen(path) as fdoc:
                        while True:
                            try:
                                yield loads(fdoc)
                            except EOFError:
                                break

            for rec in temp_loader(rec_path,
                                   hard_selectors=aka.hard_selectors):
                logger.debug(rec)
                aka.add(rec)
                total += 1
                if total % 1000 == 0:
                    elapsed = time.time() - start
                    rate = total / elapsed
                    logger.debug('%d done in %.1f sec --> %.1f per sec', 
                                total, elapsed, rate)
            logger.debug('finished %s, total recs=%d', rec_path, total)
    logger.debug('finished %d recs', total)


if __name__ == '__main__':
    main()
