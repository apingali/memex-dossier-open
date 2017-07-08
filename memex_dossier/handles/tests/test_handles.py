
from __future__ import absolute_import
import pytest

from streamcorpus_pipeline._clean_html import make_clean_html
from streamcorpus_pipeline._clean_visible import make_clean_visible

from memex_dossier.handles.classifier import extract_user_names
from memex_dossier.handles.eval_data import load_eval_data, usernames_with_saved_data

@pytest.yield_fixture(params=usernames_with_saved_data)
def eval_data(request):
    yield load_eval_data(request.param)

# TODO: think about how tests really should work for this
@pytest.mark.xfail
def test_handles(eval_data):
    for text, expected in eval_data:
        text = make_clean_html(text)
        text = make_clean_visible(text)
        sc = extract_user_names(text)

        assert set(sc) == set(expected)
