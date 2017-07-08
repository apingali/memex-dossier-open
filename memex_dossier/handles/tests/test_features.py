
import logging
import nltk
import pytest
import traceback

from memex_dossier.handles import features
from memex_dossier.models.tests import nltk_data  # noqa

logger = logging.getLogger(__name__)


@pytest.fixture(scope='session')  # noqa
def corpora(nltk_data):
    return features.initialize_corpora()


