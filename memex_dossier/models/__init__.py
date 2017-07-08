'''
.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

``memex_dossier.models`` provides search engines and a :mod:`memex_dossier.web`
application for working with active learning.

.. automodule:: memex_dossier.models.web.run
.. automodule:: memex_dossier.models.features
.. automodule:: memex_dossier.models.etl
.. automodule:: memex_dossier.models.soft_selectors
.. automodule:: memex_dossier.models.linker
'''
from memex_dossier.models import features

__all__ = [
    'features',
]
