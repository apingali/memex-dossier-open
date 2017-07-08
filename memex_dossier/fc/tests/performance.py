'''Performance tests for memex_dossier.fc.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

'''
from __future__ import absolute_import, division, print_function
import json
import time

from memex_dossier.fc import FeatureCollection
from memex_dossier.fc.feature_collection import registry, FeatureTypeRegistry
from memex_dossier.fc.tests.thing import Thing, ThingSerializer


def perftest_throughput_feature_collection():
    with registry:
        registry.add('StringCounter', ThingSerializer)
        fc = FeatureCollection()
        fc['thing1'] = Thing(json.dumps(dict(one_mb=' ' * 2**20)))
        fc_str = fc.dumps()

        start_time = time.time()
        num = 1000
        for i in range(num):
            fc2 = FeatureCollection.loads(fc_str)
            fc2.dumps()
        elapsed = time.time() - start_time
        rate = float(num) / elapsed
        print('%d MB in %.1f sec --> %.1f MB per sec' % (num, elapsed, rate))


def perftest_truncation_speed(counter_type):
    for x in range(4, 7):
        num = 10**x
        data = {unicode(x): x+1 for x in xrange(num)}

        counter = counter_type(data)

        truncation_length = 10
        start_time = time.time()
        counter.truncate_most_common(truncation_length)
        elapsed = time.time() - start_time
        rate = num / elapsed
        print('%s truncated a counter of size %d in %.3f sec '
              '--> %.1f items/sec' % (counter_type, num, elapsed, rate))
        assert len(counter) == truncation_length


def main():
    perftest_throughput_feature_collection()
    old_default = FeatureTypeRegistry.DEFAULT_FEATURE_TYPE_NAME
    for k in registry.types():
        if not ('Counter' in k or k == 'SparseVector'):
            continue
        ct = registry.get_constructor(k)
        FeatureTypeRegistry.DEFAULT_FEATURE_TYPE_NAME = k
        perftest_truncation_speed(ct)
    FeatureTypeRegistry.DEFAULT_FEATURE_TYPE_NAME = old_default


if __name__ == '__main__':
    main()
