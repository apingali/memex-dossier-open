import re
from Queue import PriorityQueue
from collections import defaultdict
import requests
from bisect import bisect_left
import subprocess
import json
import random
from functools32 import lru_cache
import traceback

class surface_search_engine():
    """ This class queries web search engines and returns a list of urls
    It is not customizable with options.
    TODO: Decide how much effort we should put into this package 
          which is related how its depenency on treelab/metasearch will be resolved if crawler becomes open source
    """

    def __init__(self, engine):
        self.command = '/home/ubuntu/treelab/metasearch/' + engine + '/search'

    @lru_cache(maxsize=10000)
    def __call__(self, query, limit=20):
        proc = subprocess.Popen([self.command],stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE)
 
        raw_results = proc.communicate(input=json.dumps({"query": '"'+query+'"', "limit": limit}))
        j = json.loads(raw_results[0])
        ret = set()
        for engine_results in j['results'].values():
            ret.update(result['abs_url'] for result in engine_results)
        length = len(ret)
        if length == 0:
            return []
        if length > limit:
            ret = random.sample(ret, limit)
        confidence = min(.9, 9./length)
        return [(r, confidence) for r in ret]

class FakeSearchEngineForTesting:
    def __init__(self, result_list):
        self.result_list = result_list

    def __call__(self, query, limit=10):
        del query
        for i in range(min(limit, len(self.result_list))):
            del i
            ret = self.result_list.pop(0)
            self.result_list.append(ret)
            yield ret, .8

def distance(base_locations, query_location, document_len):
    """ Return the minimum distance from "query_location" to one of the locations in base_locations
    it uses bisect_left, so base_locations must be sorted.
    There is some logic to upper bound the returned distance based upon document_len if base locations are empty

    Base locations should correspond to places in a returned document where the search query string appears.  
    Because web search engines use fuzzy logic, sometimes we can't (easily) figure out what strings caused
    this document to return.  In those cases we punt.
    """
    if not base_locations:
        # magic number here, what to do if search string isn't in text?
        # maybe it was in text when external search engine indexed it, but isn't now
        # maybe it's in the text in a different form
        return document_len * .25
    i = bisect_left(base_locations, query_location)
    if i==0:
        return abs(query_location - base_locations[0])
    if i == len(base_locations):
        return abs(query_location - base_locations[-1])
    return min(query_location - base_locations[i-1], base_locations[i] - query_location)
    

#  ! # $ % & ' * + - / = ? ^ _ ` { | } ~
name_chars = 'a-zA-z0-9!#$%&*+/=?^_|.-'
domain_chars = 'a-zA-Z0-9._-'
email_re = re.compile(r'['+name_chars+']+@['+domain_chars+']*')
phone_re = re.compile(r'\D(?:\(\d{3}\)|\d{3})[-. ]?\d{3}[-. ]?\d{4}\D')

def basic_extractor(raw_text, search_text):
    """ turns raw text into a record format
    returns something like {"email": ("user@name.com", .6)}
    The ".6"  in the above example is a confidence factor with 1 being the highest
    """
    search_locations = [match.start() for match in re.finditer(re.escape(search_text),
                                                               raw_text)]
    ret = {'raw_text': raw_text}
    length = min(len(raw_text), 10000) # magic number here, distances beyond this are not extracted
    
    def distance_to_confidence(d):
        if d > length:
            return 0
        return 1- (float(d) / length)
    emails = [email for email in email_re.finditer(raw_text) if len(email.group(0)) > 5]
    if emails:
        distances = [distance_to_confidence(distance(search_locations, match.start(), length))
                     for match in emails]
        email_matches = [x for x in zip([match.group(0) for match in emails], distances)
                         if x[1]>0]
        ret['email'] = email_matches
        ret['username'] = [(addr.split('@')[0], dist) for addr, dist in email_matches]

    phones = list(phone_re.finditer(raw_text))
    if phones:
        distances = [distance_to_confidence(distance(search_locations, match.start(), length))
                     for match in phones]
        phone_matches = [x for x in zip([match.group(0)[1:-1] for match in phones], distances)
                         if x[1] > 0]
        ret['phone'] = phone_matches
    
    return ret


def iterate_query_strings(extracted, base_confidence):
    """ currently this iterates over all identifiers in isolation
        in principle it could iterate over combinations like: "<name> <location"
    "extracted" should look like the output of "base_extractor"
    """
    for category, transitive_confidence in [('email', .7),
                                            ('username', .5)]:
        new_confidence = base_confidence * transitive_confidence
        if new_confidence > 0:
            for identifier in extracted.get(category, []):
                if type(identifier) is tuple:
                    identifier, multiplier = identifier
                else:
                    multiplier = 1.0
                    if len(identifier) > 6:
                        yield new_confidence * multiplier, identifier

class FakeFetcher:
    """ this class simulates a fetcher by return some basic text """
    def __init__(self, email):
        self.text = 'this is some fake text with the email ' + email

    def __call__(self, url):
        del url
        import time
        time.sleep(2)
        return self.text

class RequestsFetcher:
    """ wrap getting web pages using the requests package """
    def __init__(self, timeout=1):
        self.session = requests.session()
        self.timeout = timeout

    def __call__(self, url):
        resp = self.session.get(url)
        if resp.status_code == 200:
            return resp.content
        else:
            return None

class Crawler:
    """ Run one of these on a single extracted record, potentially in its own thread
    """

    def __init__(self, search_engine, extractor, query_string_iterator,
                 fetcher, extracted=None, search=None, confidence_threshold=.2, search_breadth=1):
        self.search_engine = search_engine
        self.extractor = extractor
        self.query_string_iterator = query_string_iterator
        self.fetcher = fetcher
        self.fetch_queue = PriorityQueue()
        self.search_queue = PriorityQueue()
        self.confidence_threshold = confidence_threshold
        self.initial_extracted=extracted or {}
        self.initial_search=search or None
        self.extra_urls = defaultdict(lambda: 0)
        self.extra_identifiers = defaultdict(lambda: 0)
        self.search_breadth = search_breadth
        self.executed_queries = set()
        self.seen_urls = set()

    def push_extracted(self, extracted, base_confidence):
        """ takes a _source of extracted features and generates a sequence of query strings for them
        """
        for confidence, query_string in self.query_string_iterator(extracted, base_confidence):
            if confidence > .2:
                self.search_queue.put((-confidence, query_string))

    def update_score(self, dictionary, key, new_score):
        # max
        dictionary[key] = max(dictionary[key], new_score)
        
        # independent probabilities
        # dictionary[keyl] = 1 - (1 - new_score)*(1-dictionary[key])

    def run(self):
        print('.')
        self.push_extracted(self.initial_extracted, 1.0)
        if self.initial_search:
            self.search_queue.put((-1, self.initial_search))

        while not self.fetch_queue.empty() or not self.search_queue.empty():
            try:
                if not self.search_queue.empty():
                    neg_confidence, query_string = self.search_queue.get()
                    if query_string in self.executed_queries:
                        continue
                    self.executed_queries.add(query_string)
                    for url, transitive_confidence in self.search_engine(query_string, self.search_breadth):
                        new_neg_confidence = neg_confidence * transitive_confidence
                        if new_neg_confidence < -self.confidence_threshold:
                            self.fetch_queue.put((new_neg_confidence, url, query_string))

                if not self.fetch_queue.empty():
                    neg_confidence, url, search_string = self.fetch_queue.get()
                    #fetched_raw = requests.get(url)
                    if url in self.seen_urls:
                        continue
                    self.seen_urls.add(url)
                    raw_text = self.fetcher(url)
                    #print(-neg_confidence, search_string, url)
                    # proper check here
                    if raw_text:
                        extracted = self.extractor(raw_text, search_string)
                        # this formula gives a waiting as if confidences were independent evidence of something:
                        # if either new informion's confidence or old confidence is 1, new confidence is 1
                        # if new info and old are both .5, new confidence is .75
                        self.update_score(self.extra_urls, url, -neg_confidence)
                        for category in extracted:
                            if category != 'raw_text':
                                for identifier, distance in extracted[category]:
                                    #label = category + '.' + identifier
                                    self.update_score(self.extra_identifiers, identifier, -neg_confidence)
                        self.push_extracted(extracted, -neg_confidence)
            except KeyboardInterrupt:
                raise
            except:
                traceback.print_exc()
        print('X')
        return self.initial_search, dict(self.extra_urls), dict(self.extra_identifiers)

def search(query, extracted, engine):
    engine = surface_search_engine(engine)
    url_fetcher = RequestsFetcher()
    
    crawler = Crawler(engine, basic_extractor, iterate_query_strings,
                      url_fetcher, search=query, extracted=extracted)
    return crawler.run()

def callback_fn((search, urls, identifiers)):
    print("search: %s\n\t%s\n\t%s\n" % (search, urls, identifiers))

def recover_username(name_and_domain):
    return '@'.join(name_and_domain.split('@')[:-1])

#ret = surface_search_engine('AnomalyA')
#import pdb; pdb.set_trace()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('filename', help='filename giving json file with profiles to read')
    parser.add_argument('outfile')
    parser.add_argument('--engine', default='surface_web', choices=['surface_web', 'google'])
    parser.add_argument('--logfile')
    args = parser.parse_args()

    with open(args.filename) as f:
        data = json.load(f)

    augmented = {}
    logfile = None
    if args.logfile:
        logfile = open(args.logfile, 'w')
    for key, value in data.items():
        try:
            username = recover_username(key)
            content = value.values()[0]['_source']['real_raw_content']
            ex = basic_extractor(content, username)
            name, urls, identifiers = search(username, ex, args.engine)
            if logfile:
                json.dump([name, urls, identifiers], logfile)
                logfile.write('\n')
                logfile.flush()
            augmented[key] = {"urls": urls, "identifiers": identifiers}
        except KeyboardInterrupt:
            with open(args.outfile, 'w') as outfile:
                json.dump(augmented, outfile)

        except Exception as e:
            print(e)
    logfile.close()
    with open(args.outfile, 'w') as outfile:
        json.dump(augmented, outfile)

    #search(['a', 'b'], callback_fn)
