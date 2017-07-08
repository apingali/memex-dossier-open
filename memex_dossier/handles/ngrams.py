'''memex_dossier.handles.ngrams computes character n-gram statistics

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

'''
from __future__ import absolute_import, print_function, division
from collections import Counter, deque
import gzip
import json
import regex as re
import sys
import time
import unicodedata

whitespace_re = re.compile(ur'(\n|\s|\p{Z})+', flags=re.UNICODE | re.MULTILINE | re.IGNORECASE)
punctuation_re = re.compile(ur'(\p{P}|\p{S})+', flags=re.UNICODE | re.MULTILINE | re.IGNORECASE)

def cleanse_whitespace(text):
    '''collapses whitespace and separators \p{Z} to a single space " "

    http://www.fileformat.info/info/unicode/category/index.htm

    '''
    return whitespace_re.sub(" ", text)

def cleanse_punctuation(text):
    '''removes all puncuation \p{P} and symbols \p{S}

    http://www.fileformat.info/info/unicode/category/index.htm

    '''
    return punctuation_re.sub(" ", text)


def ngrams(text, num=2):
    '''generates string n-grams by sliding an n-wide window over the
    `text` string.

    '''
    if len(text) > num:
        for idx in xrange(len(text) - num + 1):
            # maybe replace this with bytearray?  Test this with unicode.
            yield text[idx:idx + num]


def streaming_ngrams(text_stream, num=2):
    '''generates string n-grams by sliding an n-wide window over a stream
    of `text`.

    '''
    q = deque()
    text_stream = iter(text_stream)
    while len(q) < num:
        q.append(text_stream.next())
    while 1:
        yield u''.join(q)
        q.popleft()
        try:
            q.append(text_stream.next())
        except StopIteration:
            break

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('input')
    parser.add_argument('output', nargs='?')
    parser.add_argument('--dump', action='store_true', default=False,
                        help='dump the contents of the input file that was '
                        'generated as output of this script')
    parser.add_argument('--limit', type=int, default=None, 
                        help='stop after this many lines')
    parser.add_argument('--num', type=int, default=2,
                        help='make ngrams of this many characters')
    parser.add_argument('--lower', action='store_true', default=False,
                        help='lowercase before counting n-grams')
    parser.add_argument('--nfkc', action='store_true', default=False,
                        help='perform unicode normalization before counting n-grams')
    parser.add_argument('--cleanse-punctuation', action='store_true', default=False,
                        help='strip punctuation')
    parser.add_argument('--cleanse-whitespace', action='store_true', default=False,
                        help='collapse whitespace to " "')
    args = parser.parse_args()

    if args.input == '-':
        i_fh = sys.stdin
    elif args.input.endswith('.gz'):
        i_fh = gzip.open(args.input)
    else:
        i_fh = open(args.input)

    if args.dump:
        counter = Counter(dict(json.load(i_fh)))
        print(counter.most_common(args.limit))
        sys.exit()

    if args.output.endswith('.gz'):
        o_fh = gzip.open(args.output, 'wb')
    else:
        o_fh = open(args.output, 'wb')

    counter = Counter()
    start = time.time()
    print('digesting files from %r' % args.input)
    for idx, line in enumerate(i_fh):
        if not isinstance(line, unicode):
            try:
                line = line.decode('utf8')
            except:
                print('failed to decode %r' % line)
                continue
        if args.nfkc:
            line = unicodedata.normalize('NFKC', line)
        if args.lower:
            line = line.lower()
        if args.cleanse_punctuation:
            line = cleanse_punctuation(line)
        if args.cleanse_whitespace:
            line = cleanse_whitespace(line)
        for gram in ngrams(line, num=args.num):
            counter[gram] += 1
        if args.limit and args.limit < idx:
            break
        if idx % 100 == 0:
            elapsed = time.time() - start
            rate = idx / elapsed            
            print('%d in %.1f --> %.1f per sec' % (idx, elapsed, rate))
    o_fh.write(json.dumps(counter.most_common(), indent=4))
    o_fh.close()


if __name__ == '__main__':
    main()
