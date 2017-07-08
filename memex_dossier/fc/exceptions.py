''':mod:`memex_dossier.fc` exceptions.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

.. autoclass:: ReadOnlyException
.. autoclass:: SerializationError

'''
import logging
logger = logging.getLogger(__name__)

class BaseException(Exception):
    '''Base exception for all things in memex_dossier.fc
    '''
    pass


class ReadOnlyException(BaseException):
    '''Code attempted to modify a read-only feature collection.

    This occurs when adding, deleting, or making other in-place
    modifications to a :class:`~memex_dossier.fc.FeatureCollection` that has
    its :attr:`~memex_dossier.fc.FeatureCollection.read_only` flag set.  It
    also occurs when attempting to make changes to a
    :class:`~memex_dossier.fc.StringCounter` contained in such a collection.

    '''
    pass


class SerializationError(BaseException):
    '''A problem occurred serializing or deserializing.

    This can occur if a :class:`~memex_dossier.fc.FeatureCollection` has an
    unrecognized feature type, or if a CBOR input does not have the
    correct format.

    '''
    pass

class NonUnicodeKeyError(BaseException, TypeError):
    '''Keys in StringCounter, GeoCoords, FeatureTokens must be unicode

    '''
    def __init__(self, key):
        message = ('keys in memex_dossier.fc.{StringCounter,'
                    'GeoCoords,FeatureTokens} must be unicode')
        if key is not None:
            message += ' not: %r for %r' % (type(key), key)
        super(NonUnicodeKeyError, self).__init__(message)


def uni(key):
    '''as a crutch, we allow str-type keys, but they really should be
    unicode.

    '''
    if isinstance(key, str):
        logger.warn('assuming utf8 on: %r', key)
        return unicode(key, 'utf-8')
    elif isinstance(key, unicode):
        return key
    else:
        raise NonUnicodeKeyError(key)
