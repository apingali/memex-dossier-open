'''memex_dossier.fc Feature Collections

:class:`memex_dossier.fc.FeatureCollection` provides
convenience methods for working with collections of features
such as :class:`memex_dossier.fc.StringCounter`.

A feature collection provides ``__add__`` and ``__sub__`` so that
adding/subtracting FeatureCollections does the right thing for its
constituent features.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

'''
from __future__ import absolute_import, division, print_function
try:
    from collections import Counter
except ImportError:
    from backport_collections import Counter
from collections import MutableMapping, Sequence
import copy
from itertools import chain, ifilter, imap
import logging
import operator

import cbor
import pkg_resources

import streamcorpus

from memex_dossier.fc.exceptions import ReadOnlyException, SerializationError
from memex_dossier.fc.feature_tokens import FeatureTokens, FeatureTokensSerializer
from memex_dossier.fc.geocoords import GeoCoords, GeoCoordsSerializer
from memex_dossier.fc.string_counter import StringCounterSerializer, StringCounter
from memex_dossier.fc.vector import SparseVector, DenseVector


logger = logging.getLogger(__name__)


class FeatureCollectionChunk(streamcorpus.CborChunk):
    '''
    A :class:`FeatureCollectionChunk` provides a way to serialize
    a colllection of feature collections to a single blob of data.

    Here's an example that writes two feature collections to an existing
    file handle::

        fc1 = FeatureCollection({'NAME': {'foo': 2, 'baz': 1}})
        fc2 = FeatureCollection({'NAME': {'foo': 4, 'baz': 2}})

        fh = StringIO()
        chunk = FeatureCollectionChunk(file_obj=fh, mode='wb')
        chunk.add(fc1)
        chunk.add(fc2)
        chunk.flush()

    And the blob created inside the buffer can be read from to produce
    an iterable of the feature collections that were written::

        fh = StringIO(fh.getvalue())
        chunk = FeatureCollectionChunk(file_obj=fh, mode='rb')
        rfc1, rfc2 = list(chunk)
        assert fc1 == rfc1
        assert fc2 == rfc2
    '''
    def __init__(self, *args, **kwargs):
        kwargs['write_wrapper'] = FeatureCollection.to_dict
        kwargs['read_wrapper'] = FeatureCollection.from_dict
        kwargs['message'] = lambda x: x
        super(FeatureCollectionChunk, self).__init__(*args, **kwargs)


class FeatureCollection(MutableMapping):
    '''
    A collection of features.

    This is a dictionary from feature name to a
    :class:`collections.Counter` or similar object. In typical use
    callers will not try to instantiate individual dictionary elements,
    but will fall back on the collection's default-value behavior::

        fc = FeatureCollection()
        fc['NAME']['John Smith'] += 1

    The default default feature type is
    :class:`~memex_dossier.fc.StringCounter`.

    **Feature collection construction and serialization:**

    .. automethod:: __init__
    .. automethod:: loads
    .. automethod:: dumps
    .. automethod:: from_dict
    .. automethod:: to_dict
    .. automethod:: register_serializer

    **Feature collection values and attributes:**

    .. autoattribute:: read_only
    .. autoattribute:: generation
    .. autoattribute:: DISPLAY_PREFIX
    .. autoattribute:: EPHEMERAL_PREFIX

    **Feature collection computation:**

    .. automethod:: __add__
    .. automethod:: __sub__
    .. automethod:: __mul__
    .. automethod:: __imul__
    .. automethod:: total
    .. automethod:: merge_with
    '''
    __slots__ = ['_features', '_read_only']

    DISPLAY_PREFIX = '#'
    '''Prefix on names of features that are human-readable.

    Processing may convert a feature ``name`` to a similar feature
    ``#name`` that is human-readable, while converting the original
    feature to a form that is machine-readable only; for instance,
    replacing strings with integers for faster comparison.
    '''

    EPHEMERAL_PREFIX = '_'
    '''Prefix on names of features that are not persisted.

    :meth:`to_dict` and :meth:`dumps` will not write out features
    that begin with this character.
    '''

    TOKEN_PREFIX = '@'
    '''Prefix on names of features that contain tokens.'''


    GEOCOORDS_PREFIX = '!'
    '''Prefix on names of features that contain geocoords.'''

    @staticmethod
    def register_serializer(feature_type, obj):
        '''
        This is a **class** method that lets you define your own
        feature type serializers. ``tag`` should be the name of
        the feature type that you want to define serialization
        for. Currently, the valid values are ``StringCounter``,
        ``Unicode``, ``SparseVector`` or ``DenseVector``.

        Note that this function is not thread safe.

        ``obj`` must be an object with three attributes defined.

        ``obj.loads`` is a function that takes a CBOR created
        Python data structure and returns a new feature counter.

        ``obj.dumps`` is a function that takes a feature counter
        and returns a Python data structure that can be serialized
        by CBOR.

        ``obj.constructor`` is a function with no parameters that
        returns the Python ``type`` that can be used to construct
        new features. It should be possible to call
        ``obj.constructor()`` to get a new and empty feature
        counter.
        '''
        registry.add(feature_type, obj)

    def __init__(self, data=None, read_only=False):
        '''
        Creates a new empty feature collection.

        If ``data`` is a dictionary-like object with a structure similar
        to that of a feature collection (i.e., a dict of multisets),
        then it is used to initialize the feature collection.
        '''
        self._features = {}
        self.read_only = read_only
        if data is not None:
            self._from_dict_update(data)

    @classmethod
    def loads(cls, data):
        '''Create a feature collection from a CBOR byte string.'''
        rep = cbor.loads(data)
        if not isinstance(rep, Sequence):
            raise SerializationError('expected a CBOR list')
        if len(rep) != 2:
            raise SerializationError('expected a CBOR list of 2 items')
        metadata = rep[0]
        if 'v' not in metadata:
            raise SerializationError('no version in CBOR metadata')
        if metadata['v'] != 'fc01':
            raise SerializationError('invalid CBOR version {!r} '
                                     '(expected "fc01")'
                                     .format(metadata['v']))
        read_only = metadata.get('ro', False)
        contents = rep[1]
        return cls.from_dict(contents, read_only=read_only)

    def dumps(self):
        '''Create a CBOR byte string from a feature collection.'''
        metadata = {'v': 'fc01'}
        if self.read_only:
            metadata['ro'] = 1
        rep = [metadata, self.to_dict()]
        return cbor.dumps(rep)

    @classmethod
    def from_dict(cls, data, read_only=False):
        '''Recreate a feature collection from a dictionary.

        The dictionary is of the format dumped by :meth:`to_dict`.
        Additional information, such as whether the feature collection
        should be read-only, is not included in this dictionary, and
        is instead passed as parameters to this function.

        '''
        fc = cls(read_only=read_only)
        fc._features = {}
        fc._from_dict_update(data)
        return fc

    def _from_dict_update(self, data):
        for name, feat in data.iteritems():
            if not isinstance(name, unicode):
                name = name.decode('utf-8')
            if name.startswith(self.EPHEMERAL_PREFIX):
                self._features[name] = feat
                continue

            if isinstance(feat, cbor.Tag):
                if feat.tag not in cbor_tags_to_names:
                    raise SerializationError(
                        'Unknown CBOR value (tag id: %d): %r'
                        % (feat.tag, feat.value))
                loads = registry.get(cbor_tags_to_names[feat.tag]).loads
                feat = feat.value
            elif is_native_string_counter(feat):
                loads = registry.get('StringCounter').loads
            elif isinstance(feat, unicode):
                # A Unicode string.
                loads = registry.get('Unicode').loads
            elif registry.is_serializeable(feat):
                loads = lambda x: x
            else:
                raise SerializationError(
                    'Unknown CBOR value (type: %r): %r' % (type(feat), feat))
            value = loads(feat)
            if hasattr(value, 'read_only'):
                value.read_only = self.read_only
            self._features[name] = value

    def to_dict(self):
        '''Dump a feature collection's features to a dictionary.

        This does not include additional data, such as whether
        or not the collection is read-only.  The returned dictionary
        is suitable for serialization into JSON, CBOR, or similar
        data formats.

        '''
        def is_non_native_sc(ty, encoded):
            return (ty == 'StringCounter'
                    and not is_native_string_counter(encoded))

        fc = {}
        native = ('StringCounter', 'Unicode')
        for name, feat in self._features.iteritems():
            if name.startswith(self.EPHEMERAL_PREFIX):
                continue
            if not isinstance(name, unicode):
                name = name.decode('utf-8')
            tyname = registry.feature_type_name(name, feat)
            encoded = registry.get(tyname).dumps(feat)

            # This tomfoolery is to support *native untagged* StringCounters.
            if tyname not in native or is_non_native_sc(tyname, encoded):
                encoded = cbor.Tag(cbor_names_to_tags[tyname], encoded)
            fc[name] = encoded
        return fc

    @property
    def generation(self):
        '''Get the generation number for this feature collection.

        This is the highest generation number across all counters
        in the collection, if the counters support generation
        numbers.  This collection has not changed if the generation
        number has not changed.

        '''
        return max(getattr(collection, 'generation', 0)
                   for collection in self._features.itervalues())

    def __repr__(self):
        return 'FeatureCollection(%r)' % self._features

    def __missing__(self, key):
        default_tyname = FeatureTypeRegistry.get_default_feature_type_name(key)
        if self.read_only:
            # reaturn a read-only instance of the default type, of
            # which there is one, because we'll only ever need one,
            # because it's read-only.
            return registry.get_read_only(default_tyname)
        if key.startswith(self.EPHEMERAL_PREFIX):
            # When the feature name starts with an ephemeral prefix, then
            # we have no idea what it should be---anything goes. Therefore,
            # the caller must set the initial value themselves (and we must
            # be careful not to get too eager with relying on __missing__).
            raise KeyError(key)
        default_value = registry.get_constructor(default_tyname)()
        self[key] = default_value
        return default_value

    def __contains__(self, key):
        '''
        Returns ``True`` if the feature named ``key`` is in this
        FeatureCollection.

        :type key: unicode
        '''
        return key in self._features

    def merge_with(self, other, multiset_op, other_op=None):
        '''Merge this feature collection with another.

        Merges two feature collections using the given ``multiset_op``
        on each corresponding multiset and returns a new
        :class:`FeatureCollection`. The contents of the two original
        feature collections are not modified.

        For each feature name in both feature sets, if either feature
        collection being merged has a :class:`collections.Counter`
        instance as its value, then the two values are merged by
        calling `multiset_op` with both values as parameters.  If
        either feature collection has something other than a
        :class:`collections.Counter`, and `other_op` is not
        :const:`None`, then `other_op` is called with both values to
        merge them.  If `other_op` is :const:`None` and a feature
        is not present in either feature collection with a counter
        value, then the feature will not be present in the result.

        :param other: The feature collection to merge into ``self``.
        :type other: :class:`FeatureCollection`
        :param multiset_op: Function to merge two counters
        :type multiset_op: fun(Counter, Counter) -> Counter
        :param other_op: Function to merge two non-counters
        :type other_op: fun(object, object) -> object
        :rtype: :class:`FeatureCollection`

        '''
        result = FeatureCollection()
        for ms_name in set(self._counters()) | set(other._counters()):
            c1 = self.get(ms_name, None)
            c2 = other.get(ms_name, None)
            if c1 is None and c2 is not None:
                c1 = c2.__class__()
            if c2 is None and c1 is not None:
                c2 = c1.__class__()
            result[ms_name] = multiset_op(c1, c2)
        if other_op is not None:
            for o_name in (set(self._not_counters()) |
                           set(other._not_counters())):
                v = other_op(self.get(o_name, None), other.get(o_name, None))
                if v is not None:
                    result[o_name] = v
        return result

    def __add__(self, other):
        '''
        Add features from two FeatureCollections.

        >>> fc1 = FeatureCollection({'foo': Counter('abbb')})
        >>> fc2 = FeatureCollection({'foo': Counter('bcc')})
        >>> fc1 + fc2
        FeatureCollection({'foo': Counter({'b': 4, 'c': 2, 'a': 1})})

        Note that if a feature in either of the collections is not an
        instance of :class:`collections.Counter`, then it is ignored.
        '''
        return self.merge_with(other, operator.add)

    def __iadd__(self, other):
        '''
        In-place add features from two FeatureCollections.

        >>> fc1 = FeatureCollection({'foo': Counter('abbb')})
        >>> fc2 = FeatureCollection({'foo': Counter('bcc')})
        >>> fc1 += fc2
        FeatureCollection({'foo': Counter({'b': 4, 'c': 2, 'a': 1})})

        Note that if a feature in either of the collections is not an
        instance of :class:`collections.Counter`, then it is ignored.
        '''
        if self.read_only:
            raise ReadOnlyException()
        fc = self.merge_with(other, operator.iadd)
        self._features = fc._features
        return self

    def __sub__(self, other):
        '''
        Subtract features from two FeatureCollections.

        >>> fc1 = FeatureCollection({'foo': Counter('abbb')})
        >>> fc2 = FeatureCollection({'foo': Counter('bcc')})
        >>> fc1 - fc2
        FeatureCollection({'foo': Counter({'b': 2, 'a': 1})})

        Note that if a feature in either of the collections is not an
        instance of :class:`collections.Counter`, then it is ignored.
        '''
        return self.merge_with(other, operator.sub)

    def __isub__(self, other):
        '''
        In-place subtract features from two FeatureCollections.

        >>> fc1 = FeatureCollection({'foo': Counter('abbb')})
        >>> fc2 = FeatureCollection({'foo': Counter('bcc')})
        >>> fc1 -= fc2
        FeatureCollection({'foo': Counter({'b': 2, 'a': 1})})

        Note that if a feature in either of the collections is not an
        instance of :class:`collections.Counter`, then it is ignored.
        '''
        if self.read_only:
            raise ReadOnlyException()
        fc = self.merge_with(other, operator.isub)
        self._features = fc._features
        return self

    def __mul__(self, coef):
        fc = copy.deepcopy(self)
        fc *= coef
        return fc

    def __imul__(self, coef):
        '''In-place multiplication by a scalar.'''
        if self.read_only:
            raise ReadOnlyException()
        if coef == 1:
            return self
        for name in self._counters():
            self[name] *= coef
        return self

    def total(self):
        '''
        Returns sum of all counts in all features that are multisets.
        '''
        feats = imap(lambda name: self[name], self._counters())
        return sum(chain(*map(lambda mset: map(abs, mset.values()), feats)))

    def __getitem__(self, key):
        '''
        Returns the feature named ``key``. If ``key`` does not
        exist, then a new empty :class:`StringCounter` is created.
        Alternatively, a second optional positional argument can
        be given that is used as a default value.

        Note that traditional indexing is supported too, e.g.::

            fc = FeatureCollection({'NAME': {'foo': 1}})
            assert fc['NAME'] == StringCounter({'foo': 1})

        :type key: unicode
        '''
        v = self._features.get(key)
        if v is not None:
            return v
        return self.__missing__(key)

    def get(self, key, default=None):
        if key not in self:
            return default
        else:
            return self._features[key]

    def __setitem__(self, key, value):
        if self.read_only:
            raise ReadOnlyException()
        if not isinstance(key, unicode):
            if isinstance(key, str):
                key = unicode(key)
            else:
                raise TypeError(key)
        if hasattr(value, 'read_only'):
            value.read_only = self.read_only
        self._features[key] = value

    def __delitem__(self, key):
        '''
        Deletes the feature named ``key`` from this feature collection.

        :type key: unicode
        '''
        if self.read_only:
            raise ReadOnlyException()
        if key in self._features:
            del self._features[key]
        else:
            raise KeyError(key)

    def _counters(self):
        '''
        Returns a generator of ``feature_name`` where ``feature_name``
        corresponds to a feature that is an instance of
        :class:`collections.Counter`. This method is useful when you
        need to perform operations only on features that are multisets.
        '''
        return ifilter(lambda n: is_counter(self[n]), self)

    def _not_counters(self):
        '''
        Returns a generator of ``feature_name`` where ``feature_name``
        corresponds to a feature that is **not** an instance of
        :class:`collections.Counter`. This method is useful when you
        need to perform operations only on features that are **not**
        multisets.
        '''
        return ifilter(lambda n: not is_counter(self[n]), self)

    def __iter__(self):
        return self._features.iterkeys()

    def __len__(self):
        '''
        Returns the number of features in this collection.
        '''
        return len(set(iter(self)))

    @property
    def read_only(self):
        '''Flag if this feature collection is read-only.

        When a feature collection is read-only, no part of it
        can be modified.  Individual feature counters cannot
        be added, deleted, or changed.  This attribute is
        preserved across serialization and deserialization.

        '''
        return self._read_only

    @read_only.setter
    def read_only(self, ro):
        self._read_only = ro
        for (k, v) in self._features.iteritems():
            if hasattr(v, 'read_only'):
                v.read_only = ro


class FeatureTypeRegistry (object):
    '''
    This is a pretty bogus class that has exactly one instance. Its
    purpose is to guarantee the correct lazy loading of entry points
    into the registry.
    '''
    ENTRY_POINT_GROUP = 'memex_dossier.fc.feature_types'
    DEFAULT_FEATURE_TYPE_NAME = 'StringCounter'
    DEFAULT_FEATURE_TYPE_PREFIX_NAMES = {
        FeatureCollection.DISPLAY_PREFIX: 'StringCounter',
        FeatureCollection.EPHEMERAL_PREFIX: 'StringCounter',
        FeatureCollection.TOKEN_PREFIX: 'FeatureTokens',
        FeatureCollection.GEOCOORDS_PREFIX: 'GeoCoords',
    }

    @classmethod
    def get_default_feature_type_name(cls, feature_name):
        by_prefix = cls.DEFAULT_FEATURE_TYPE_PREFIX_NAMES.get(feature_name[0])
        if by_prefix is None:
            return cls.DEFAULT_FEATURE_TYPE_NAME
        else:
            return by_prefix

    def __init__(self):
        self._registry = {}
        self._inverse = {}
        self._entry_points = False

    def add(self, name, obj):
        '''Register a new feature serializer.

        The feature type should be one of the fixed set of feature
        representations, and `name` should be one of ``StringCounter``,
        ``SparseVector``, or ``DenseVector``.  `obj` is a describing
        object with three fields: `constructor` is a callable that
        creates an empty instance of the representation; `dumps` is
        a callable that takes an instance of the representation and
        returns a JSON-compatible form made of purely primitive
        objects (lists, dictionaries, strings, numbers); and `loads`
        is a callable that takes the response from `dumps` and recreates
        the original representation.

        Note that ``obj.constructor()`` *must* return an
        object that is an instance of one of the following
        types: ``unicode``, :class:`memex_dossier.fc.StringCounter`,
        :class:`memex_dossier.fc.SparseVector` or
        :class:`memex_dossier.fc.DenseVector`. If it isn't, a
        :exc:`ValueError` is raised.
        '''
        ro = obj.constructor()
        if name not in cbor_names_to_tags:
            print(name)
            raise ValueError(
                'Unsupported feature type name: "%s". '
                'Allowed feature type names: %r'
                % (name, cbor_names_to_tags.keys()))
        if not is_valid_feature_instance(ro):
            raise ValueError(
                'Constructor for "%s" returned "%r" which has an unknown '
                'sub type "%r". (mro: %r). Object must be an instance of '
                'one of the allowed types: %r'
                % (name, ro, type(ro), type(ro).mro(), ALLOWED_FEATURE_TYPES))
        self._registry[name] = {'obj': obj, 'ro': obj.constructor()}
        self._inverse[obj.constructor] = name

    def feature_type_name(self, feat_name, obj):
        ty = type(obj)
        if ty not in self._inverse:
            raise SerializationError(
                'Python type "%s" is not in the feature type registry: %r\n\n'
                'The offending feature (%s): %r'
                % (ty, self._inverse, feat_name, obj))
        return self._inverse[ty]

    def is_serializeable(self, obj):
        return type(obj) in self._inverse

    def _get(self, type_name):
        self._load_entry_points()
        if type_name not in self._registry:
            raise SerializationError(
                'Feature type %r not in feature type registry: %r'
                % (type_name, self._registry))
        return self._registry[type_name]

    def get(self, type_name):
        return self._get(type_name)['obj']

    def get_constructor(self, type_name):
        return self._get(type_name)['obj'].constructor

    def get_read_only(self, type_name):
        return self._get(type_name)['ro']

    def types(self):
        self._load_entry_points()
        return self._registry.keys()

    def _load_entry_points(self):
        if self._entry_points:
            return
        self._entry_points = True

        for epoint in pkg_resources.iter_entry_points(self.ENTRY_POINT_GROUP):
            try:
                obj = epoint.load()
            except (ImportError, pkg_resources.DistributionNotFound):
                import traceback
                logger.warn(traceback.format_exc())
                continue
            self.add(epoint.name, obj)

    def _reset(self):
        self._entry_points = False
        self._registry = {}
        self._inverse = {}
        self.add('StringCounter', StringCounterSerializer)
        self.add('Unicode', UnicodeSerializer)
        self.add('GeoCoords', GeoCoordsSerializer)
        self.add('FeatureTokens', FeatureTokensSerializer)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self._reset()


class UnicodeSerializer(object):
    def __init__(self):
        raise NotImplementedError()

    # cbor natively supports Unicode, so we can use the identity function.
    loads = staticmethod(lambda x: x)
    dumps = staticmethod(lambda x: x)
    constructor = unicode


cbor_names_to_tags = {
    # Tagged just in case someone wants to changed the binary format.
    # By default, FeatureCollection will deserialize it as a special case
    # into a dict of {unicode |--> int} with no tag.
    'StringCounter': 55800,
    # Not tagged because CBOR supports it natively.
    'Unicode': None,
    # These are *always* tagged.
    'SparseVector': 55801,
    'DenseVector': 55802,
    # 55803 was FeatureOffsets
    'FeatureTokens': 55804,
    'GeoCoords': 55805,
}
cbor_tags_to_names = {}
for k, v in cbor_names_to_tags.items():
    if v:
        cbor_tags_to_names[v] = k
ALLOWED_FEATURE_TYPES = (
    unicode, StringCounter, SparseVector, DenseVector,
    FeatureTokens,
    GeoCoords,
)


def is_native_string_counter(cbor_data):
    return isinstance(cbor_data, dict)


def is_counter(obj):
    return isinstance(obj, Counter) \
        or getattr(obj, 'is_counter', False)


def is_valid_feature_instance(obj):
    return isinstance(obj, ALLOWED_FEATURE_TYPES)


registry = FeatureTypeRegistry()
registry._reset()
