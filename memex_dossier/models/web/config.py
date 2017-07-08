from __future__ import absolute_import, division, print_function
import logging

from gensim import models

from memex_dossier.models.openquery.google import Google
from memex_dossier.akagraph import AKAGraph
import memex_dossier.web as web
from memex_dossier.handles.char_ngram_model import load_ngrams


logger = logging.getLogger(__name__)

class Config(web.Config):
    def __init__(self, *args, **kwargs):
        super(Config, self).__init__(*args, **kwargs)
        self._tfidf = None
        self._akagraph = None
        self._google = None

    @property
    def config_name(self):
        return 'memex_dossier.models'

    def normalize_config(self, config):
        super(Config, self).normalize_config(config)
        try:
            tfidf_path = self.config['tfidf_path']
        except KeyError:
            self._tfidf = False  # service available but absent
        else:
            self._tfidf = models.TfidfModel.load(tfidf_path)

        akagraph_config = self.config.get('akagraph')
        if akagraph_config:
            self._akagraph = AKAGraph(
                akagraph_config['hosts'], akagraph_config['index_name'],
                akagraph_config['k_replicas'],
                # could make hyper_edge_scorer configurable here
                soft_selectors=akagraph_config.get('soft_selectors'),
                hard_selectors=akagraph_config.get('hard_selectors'),
            )
        else:
            self._akagraph = None

    @property
    def tfidf(self):
        return self._tfidf

    @property
    def akagraph(self):
        return self._akagraph

    @property
    def google(self):
        if self._google is None:
            api_key = self.config.get('google_api_search_key')
            if api_key is None:
                logger.warn('failed to get google_api_search_key from config')
                self._google = None
            else:
                self._google = Google(api_key)
        return self._google
