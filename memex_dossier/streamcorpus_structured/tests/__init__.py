'''tools for tests for structured_features

.. Your use of this software is governed by your license agreement.
   Unpublished Work Copyright 2015 Diffeo, Inc.
'''
from __future__ import absolute_import
import contextlib
import pytest

import yakonfig

from streamcorpus_pipeline._tokenizer import nltk_tokenizer
from streamcorpus import make_stream_item

from memex_dossier.streamcorpus_structured.transform import structured_features

structured_features_config = {
    'tagger_id_to_create': 'structured-cyber',
}

def setup_nltk(text, run_extractor=True):
    si = make_stream_item(0, '')
    si.body.clean_visible = text
    nt = nltk_tokenizer({})
    nt(si, {})

    regex_extractor = structured_features(structured_features_config)
    if run_extractor:
        regex_extractor(si)
    return si

@pytest.fixture(scope='function')
def configurator(request, namespace_string):
    base_config = {
        'structured_features': structured_features_config,
        'streamcorpus_pipeline': {
            'to_kvlayer': {},
            'from_kvlayer': {},
        },
    }

    @contextlib.contextmanager
    def make_config(overlay={}):
        config = yakonfig.merge.overlay_config(base_config, overlay)
        with yakonfig.defaulted_config([], config=config):
            yield
    return make_config
