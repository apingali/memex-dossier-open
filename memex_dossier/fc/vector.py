from __future__ import absolute_import, division, print_function
import abc


class SparseVector(object):
    '''An abstract class for sparse vectors.

    Currently, there is no default implementation of a sparse vector.

    Other implementations of sparse vectors *must* inherit
    from this class. Otherwise they cannot be used inside a
    :class:`memex_dossier.fc.FeatureCollection`.
    '''
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self): pass


class DenseVector(object):
    '''An abstract class for dense vectors.

    Currently, there is no default implementation of a dense vector.

    Other implementations of dense vectors *must* inherit
    from this class. Otherwise they cannot be used inside a
    :class:`memex_dossier.fc.FeatureCollection`.
    '''
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self): pass
