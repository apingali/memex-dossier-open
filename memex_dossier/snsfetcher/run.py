'''memex_dossier.snsfetcher for scraping Social Networking Sites

.. This software is released under an MIT/X11 open source license.
   Unpublished Work Copyright 2015 Diffeo, Inc.
'''
from __future__ import division, print_function, absolute_import
import argparse
from itertools import islice
import random
import sys
import time

import dateutil.parser
import requests
import streamcorpus

user_agents = [
'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36Chrome 45.0',
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36Chrome 45.0',
'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36Chrome 45.0',
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/600.8.9 (KHTML, like Gecko) Version/8.0.8 Safari/600.8.9',
]

headers = {
#'Connection': 'keep-alive',
'Pragma': 'no-cache',
'Cache-Control': 'no-cache',
'Accept': '*/*', #text/vnd.wap.wml', #'text/html', #,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8
'User-Agent': random.choice(user_agents),  #'curl/7.35.0'
'Accept-Encoding': 'gzip, deflate, sdch',
'Accept-Language': 'ru-RU,ru;q=0.8',  #en-US,en;q=0.8
}

def fetch_all(urls, out_dir):
    session = requests.Session()
    roller = streamcorpus.ChunkRoller(out_dir)
    start = time.time()
    try:
        for idx, url in enumerate(urls):
            print('# starting fetch of %r' % url)
            sys.stdout.flush()
            resp = requests.get(url, headers=headers)
            last_modified = resp.headers.get('last-modified')
            if last_modified:
                try:
                    last_modified = int(last_modified)
                except:
                    dt = dateutil.parser.parse(last_modified)
                    last_modified = int(dt.strftime('%s'))
            si = streamcorpus.make_stream_item(last_modified or time.time(), url)
            si.body.raw = resp.content
            si.body.media_type = resp.headers.get('content-type')
            si.body.encoding = resp.encoding
            roller.add(si)
            print('fetched %d bytes for %s with last_modified=%r' % (len(si.body.raw), url, last_modified))
            if idx % 10 == 0:
                elapsed = time.time() - start
                rate = (idx + 1) / elapsed
                remaining = (len(urls) - 1 - idx) / rate / 3600
                print('%d of %d done in %.3f seconds --> %.3f per second --> %.3f hours remaining' % ((idx + 1), len(urls), elapsed, rate, remaining))
                sys.stdout.flush()
    except:
        roller.close()
        raise
    roller.close()

def make_urls(start, end, limit=None, skip=None):

    numbers = range(start, end)
    url = 'http://'
    if skip:
        ids = set(map(int, open(skip).read().splitlines()))
        numbers = list(set(numbers) - ids)
    random.shuffle(numbers)
    for num in islice(numbers, limit):
        yield url % num


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('out_dir')
    parser.add_argument('start', type=int)
    parser.add_argument('end', type=int)
    parser.add_argument('--limit', type=int)
    parser.add_argument('--skip', help='file path to URLs to skip')
    args = parser.parse_args()

    urls = make_urls(args.start, args.end, limit=args.limit, skip=args.skip)
    fetch_all(urls, args.out_dir)


if __name__ == '__main__':
    main()
