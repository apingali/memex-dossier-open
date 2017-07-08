'''
Feature extraction
==================
``memex_dossier.models.features`` provides some convenience functions for
feature extraction.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

.. autofunction:: noun_phrases
.. autofunction:: entity_names
.. autofunction:: skypes
.. autofunction:: host_names
.. autofunction:: usernames
.. autofunction:: emails
.. autofunction:: phones
.. autofunction:: image_urls
'''
from __future__ import absolute_import
from memex_dossier.models.features.basic import emails, image_urls, phones, a_urls, skypes
from memex_dossier.models.features.basic import host_names, path_dirs
from memex_dossier.models.features.sip import noun_phrases, sip_noun_phrases
from memex_dossier.models.features.stopwords import stopwords
from memex_dossier.models.features._names import entity_names

from memex_dossier.extraction import usernames

__all__ = [
    'emails', 'image_urls', 'a_urls', 'phones',
    'noun_phrases', 'sip_noun_phrases',
    'entity_names',
    'skypes',
    'stopwords', 'host_names', 'path_dirs', 'usernames',
]
