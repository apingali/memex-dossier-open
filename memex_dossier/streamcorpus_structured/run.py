'''memex_dossier.streamcorpus_structured.run provides the shell command
memex_dossier.structured for processing streamcorpus.Chunk files and dumping
CBOR records for loading into memex_dossier.akagraph

.. Your use of this software is governed by your license agreement.
   Unpublished Work Copyright 2015 Diffeo, Inc.

'''
from __future__ import division, print_function, absolute_import
import argparse
import gzip
import sys
import time
from urlparse import urlparse
import unicodedata

import cbor
from streamcorpus import Chunk
from memex_dossier.streamcorpus_structured.page_extractors import profile_page
from memex_dossier.streamcorpus_structured.transform import structured_features


def main():
    parser = argparse.ArgumentParser(
        'process streamcorpus.Chunk files to generate CBOR files'
        ' to load into memex_dossier.akagraph.'                                     
    )
    parser.add_argument('input_paths', nargs='+', 
                        help='paths to streamcorpus.Chunk files')
    parser.add_argument('--output-path', help='cbor file (or cbor.gz) to create')
    parser.add_argument('--xform', action='store_true', default=False,
                        help='run structured_features transform before page_extractors')
    parser.add_argument('--total', type=int, help='anticipated number of StreamItems')
    parser.add_argument('--limit', type=int, 
                        help='stop processing after this many StreamItems')
    args = parser.parse_args()

    xform = structured_features(structured_features.default_config)

    fopen = open
    if args.output_path.endswith('.gz'):
        fopen = gzip.open
    fh = fopen(args.output_path, 'wb')

    count = 0
    start = time.time()
    for path in args.input_paths:
        for si in Chunk(path):
            count += 1
            if count % 100 == 0:
                elapsed = time.time() - start
                rate = count / elapsed
                msg = '%d done in %.1f secs --> %.1f per sec' % (count, elapsed, rate)
                if args.total:
                    remaining = (args.total - count) / rate
                    msg += ' --> %.1f sec remaining' % remaining
                print(msg)
                sys.stdout.flush()
            if args.limit and count > args.limit:
                break
            #url_parts = urlparse(si.abs_url)
            if args.xform:
                si = xform(si)
            slots = profile_page(si)
            if slots:
                slots = cbor.loads(slots)
                better_slots = {}
                for key, values in slots['slots'].iteritems():
                    assert isinstance(values, list), values
                    better_slots[key.lower()] = [unicodedata.normalize('NFKC', v).lower()
                                                 for v in values]
                better_slots['url'] = si.abs_url
                cbor.dump(better_slots, fh)
    fh.close()
    print('done')


if __name__ == '__main__':
    main()
