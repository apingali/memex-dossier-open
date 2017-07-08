'''
memex_dossier.web provides REST web services for Memex_Dossier Stack
========================================================

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

.. autoclass:: memex_dossier.web.WebBuilder

Here are the available search engines by default:

.. autoclass:: memex_dossier.web.search_engines.plain_index_scan
.. autoclass:: memex_dossier.web.search_engines.random

Here are the available filter predicates by default:

.. autoclass:: memex_dossier.web.filters.already_labeled
.. autoclass:: memex_dossier.web.filters.nilsimsa_near_duplicates

Some useful utility functions.

.. autofunction:: memex_dossier.web.streaming_sample

.. automodule:: memex_dossier.web.interface
.. automodule:: memex_dossier.web.routes
.. automodule:: memex_dossier.web.folder
.. automodule:: memex_dossier.web.tags
'''
from memex_dossier.web.builder import WebBuilder, add_cli_arguments
from memex_dossier.web.config import Config
from memex_dossier.web.filters import already_labeled as filter_already_labeled
from memex_dossier.web.filters import \
    nilsimsa_near_duplicates as filter_nilsimsa_near_duplicates
from memex_dossier.web.folder import Folders
from memex_dossier.web.interface import SearchEngine, Filter
from memex_dossier.web.search_engines import random as engine_random
from memex_dossier.web.search_engines import plain_index_scan as engine_index_scan
from memex_dossier.web.search_engines import streaming_sample

__all__ = [
    'WebBuilder', 'add_cli_arguments',
    'Config',
    'Folders',
    'SearchEngine', 'Filter',
    'filter_already_labeled', 'filter_nilsimsa_near_duplicates',
    'engine_random', 'engine_index_scan',
    'streaming_sample',
]
