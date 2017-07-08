'''Test for the primary external interface " streamcorpus_pipeline
transform called `structured_features`

.. Your use of this software is governed by your license agreement.
   Unpublished Work Copyright 2015 Diffeo, Inc.

'''
from __future__ import absolute_import

from memex_dossier.streamcorpus_structured.transform import structured_features
from memex_dossier.streamcorpus_structured.tests import setup_nltk, configurator


example_text = '''
Bob <bob@bob.com>
'''

def test_streamcorpus_structured_features(configurator):
    with configurator():
        si = setup_nltk(example_text, run_extractor=False)
        xform = structured_features(structured_features.default_config)
        si = xform(si)
        assert si.body.selectors




