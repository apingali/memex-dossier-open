# -*- coding: utf-8 -*-
"""
This script reads new content from Elasticsearch /onions/<doctype>/
and moves it to the another Elasticsearch server.

It only moves the content that is not older than a X days.

Can be run on a daily crontab job.
"""
import os
# os.environ['VIRTUAL_ENV']="/home/local/Elastic/lights/memex_tools/venv"
# os.environ['PATH']="$VIRTUAL_ENV/bin:$PATH"

from elasticsearch import Elasticsearch
import time
from datetime import datetime, timedelta
# use certifi for CA certificates
import certifi
# To handle command line arguments
import sys



def printErrorAndQuit():
    """Printing the usage information"""
    print "Copy data from one Elasticsearch schema to another between databases.\n"
    print "Usage: python es_copy.py TIME_RANGE DOC_TYPE_READ [DOC_TYPE_WRITE]"
    print "TIME_RANGE is the number of days from now."
    print "DOC_TYPE_READ is the item type in the read database."
    print "DOC_TYPE_WRITE is the item type in the write database."
    print "\t DOC_TYPE_WRITE is optional. Default is DOC_TYPE_WRITE=DOC_TYPE_READ.\n"
    sys.exit()

def main():
    try:
        if len(sys.argv) == 4:
            TIME_RANGE = int(sys.argv[1])
            INDEX_TYPE = str(sys.argv[2])
            DOC_TYPE_READ = str(sys.argv[3])
            DOC_TYPE_WRITE = DOC_TYPE_READ
        elif len(sys.argv) == 5:
            TIME_RANGE = int(sys.argv[1])
            INDEX_TYPE = str(sys.argv[2])
            DOC_TYPE_READ = str(sys.argv[3])
            DOC_TYPE_WRITE = str(sys.argv[4])
        else:
            printErrorAndQuit()
    except Exception,e:
        print str(e)
        printErrorAndQuit()


    # Read from this
    # Elasticsearch connection to localhost:9200
    es_read = Elasticsearch(timeout=60)

    # Write to this

    # replace <PASSWORD> with the real passwd 
    es_write = Elasticsearch(
        ['els.istresearch.com'],
        http_auth=('memex', '<PASSWORD>'),
        port=19200,
        timeout=60,
        use_ssl=True,
        verify_certs=True,
        ca_certs=certifi.where(),
    )

    # Time now
    time_now = int(round(time.time() * 1000))

    # Status of weapons data on WRITE index
    res = es_write.search(index=INDEX_TYPE, doc_type=DOC_TYPE_WRITE, body={"query": {"match_all": {}}})
    size = res['hits']['total']
    print "WRITE Index %s/%s total size is %d" % (INDEX_TYPE, DOC_TYPE_WRITE, size)

    yesterday = datetime.now() - timedelta(days=TIME_RANGE)
    yesterday = yesterday.strftime("%Y-%m-%dT%H:%M:%S")

    query = """{
        "query" : {
            "filtered" : {
                "filter" : {
                    "range" : {
                        "timestamp" : {
                            "gt" : "%s",
                            "lt" : "2100-06-13T00:00:00"}
                        }
                    }
                }
            }
        }
    }""" % yesterday

    # Status of doc_type in the READ onion index
    res = es_read.search(index="onions", doc_type=DOC_TYPE_READ, body=query)
    size = res['hits']['total']
    print "READ Index onions/%s size in this range is %d" % (DOC_TYPE_READ, size)


    start = 0
    limit = 100
    added = 0
    updated = 0
    while start < size:
        try:
            res = es_read.search(index="onions", doc_type=DOC_TYPE_READ, from_=start, size=limit, body=query)
        except Exception as e:
            print e
            continue
        print "range=%d-%d, hits %d" % (start, start+limit, len(res['hits']['hits']))
        for hit in res['hits']['hits']:
            item = hit["_source"]
            item["timestamp"] = item.get("timestamp", time_now)
            item["team"] = "SRI"
            item["crawler"] = "onionElasticBot"
            item["raw_content"] = item["html"]
            del item["html"]
            item["crawl_data"] = {
                                   "text": item["text"],
                                   "links": item["links"],
                                   "title": item["title"],
                                   "h1": item["h1"],
                                   "h2": item["h2"],
                                   "domain": item["domain"],
                                   "url": item["url"],
                                   "bitcoin_addresses": item.get("bitcoin_addresses", ""),
                                   "email_addresses": item.get("email_addresses", ""),
                                   "category": item.get("category", ""),
                                   "language": item.get("language", ""),
                                   "text": item["text"],
                }
            del item["text"]
            del item["links"]
            del item["title"]
            del item["h1"]
            del item["h2"]
            del item["domain"]
            # del item["url"]
            if 'bitcoin_addresses' in item:
                del item["bitcoin_addresses"]
            if 'email_addresses' in item:
                del item["email_addresses"]
            if 'category' in item:
                del item["category"]
            if 'language' in item:
                del item["language"]
            del item["header"]

            # Check if there is this data already
            q = 'url:"%s"' % item['url']
            try:
                res2 = es_write.search(index=INDEX_TYPE, doc_type=DOC_TYPE_WRITE, q=q, fields="url")
            except Exception as e:
                print e
                continue
            if res2['hits']['total'] == 0:
                try:
                    # Create object to another Elasticsearch
                    print "UPDATE url=%s" % (item["url"])
                    res2 = es_write.index(index=INDEX_TYPE, doc_type=DOC_TYPE_WRITE, body=item)
                    print "Created: %s" % str(res2['created'])
                    added = added + 1
                except Exception as e:
                    print e
                    continue
            elif res2['hits']['total'] > 0:
                try:
                    # Update object to another Elasticsearch
                    item_id = res2['hits']['hits'][0]["_id"]
                    print "UPDATE id=%s / url=%s" % (item_id, item["url"])
                    doc = {"doc" : item}
                    es_write.update(index=INDEX_TYPE, doc_type=DOC_TYPE_WRITE, id=item_id, body=doc)
                    updated = updated + 1
                except Exception as e:
                    print e
                    continue
        start = start + limit

    print "Objects added %d" % added
    print "Objects updated %d" % updated


if __name__ == '__main__':
    main()
