'''converts Diffeo StreamItem's into AKA Graph records

'''
from __future__ import division
from operator import itemgetter
import hashlib
import sys
import json
import os
import logging
import regex as re
import time

from treelab.streamcorpus import IngestClient, ChunkReader

from memex_dossier.handles import load_ngrams, prob_username

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

ingest_client = IngestClient('http://localhost:57313/document', timeout=500, max_retries=5)

def add(rec, field, value):
    if field not in rec:
        rec[field] = set()
    rec[field].add(value)

def tag_stream_items(path):
    batch = []
    with ChunkReader.from_path(path) as chunk:
        for si in chunk:
            if len(batch) > 2:
                resp = ingest_client.enrich(batch)
                for _si in batch:
                    try:
                        yield resp.stream_items[_si.id]
                    except Exception as exc:
                        if 'license' in exc:
                            logger.warn('trapping: %s', exc)
                batch = []
            batch.append(si)
    if batch:
        resp = ingest_client.enrich(batch)
        for _si in batch:
            try:
                yield resp.stream_items[_si.id]
            except Exception as exc:
                if 'license' in exc:
                    logger.warn('trapping: %s', exc)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('input')
    parser.add_argument('--ngrams')
    args = parser.parse_args()

    ## load the char_ngram dictionaries
    char_unigrams, char_bigrams = load_ngrams(args.ngrams)

    for si in tag_stream_items(args.input):

        rec = {
            'url': si.abs_url
        }
        num_ids = 0
        for smid, mc in si.doc.mention_chains.iteritems():
            #import pdb; pdb.set_trace()
            logger.info(mc.classes)
            for ec in mc.classes:
                class_path = ec.path
                if class_path.named[:2] == [u'Artifact', u'Cyber']:
                    identifier_type = class_path.named[2].lower()
                    if identifier_type in set(['email', 'phone']):
                        for m in mc.mentions:
                            add(rec, identifier_type, m.canonical)
                            num_ids += 1
                            if identifier_type == 'email':
                                parts = m.canonical.lower().split('@')
                                add(rec, 'username', parts[0])
                elif class_path.named[0] in 'Person':
                    for m in mc.mentions:
                        if len(re.sub('(\s|\n)+', ' ', m.raw)) / len(m.raw) < 0.6:
                            # skip things with 40% or more whitespace
                            continue
                        add(rec, 'name', m.raw)
        total = 0
        start = time.time()
        for tok in si.doc.tokens:
            total += 1
            if prob_username(tok.raw, char_unigrams, char_bigrams) > 0.5:
                add(rec, 'username', tok.raw)
        elapsed = time.time() - start
        if elapsed > 0:
            rate = total / elapsed
            logger.info('%d tokens in %.4f seconds --> %.4f per sec',
                        total, elapsed, rate)

        json_rec = {}
        for key, value in rec.items():
            if isinstance(value, set):
                json_rec[key] = list(value)
            else:
                json_rec[key] = value
        json_rec['text'] = si.raw_html
        json_rec['streamitem'] = si.to_dict()
        if num_ids > 0:
            print json.dumps(json_rec)
            logger.info( 'wrote ' + si.abs_url )

        else:
            logger.info('failed to find any identifiers in %r', si.abs_url)


    
if __name__ == '__main__':
    main()
