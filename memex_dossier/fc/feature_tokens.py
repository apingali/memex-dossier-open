'''FeatureTokens carries information connecting feature strings back
to :class:`~streamcorpus.Offset` objects in `StreamItems.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.
'''
from __future__ import absolute_import, division, print_function

from collections import MutableMapping
from itertools import ifilter, imap

from streamcorpus import OffsetType, XpathRange


class FeatureTokens(MutableMapping):
    '''Contains token pointers for features into source material.

    This abstraction represents a map from feature name to a sequence
    of token pointers. That is, each feature name maps to zero or
    more sequences of tokens, where each token sequence corresponds
    to a possibly non-contiguous region of text in the source material.
    This region of text should correspond to the place where the
    feature was extracted.

    That is, feature tokens is a map from feature name to sequences of
    token pointers. Each sequence of token pointers should correspond
    to a possibly non-contiguous region of text in the original source
    text.  Model this by using a contiguous sequence of tokens would
    fail to capture some real data, so a full sequence of tokens is
    required.

    In the world of ideas, a single sequence of token pointers should
    correspond to exactly one feature value in a document. Retrieving
    the feature value from the original HTML can then be done by
    concatenating each of the corresponding tokens.  Recall that every
    token has a *pair* of xpaths, which define a range of text in the
    HTML.

    See :meth:`tokens`, which is probably the most useful here.

    '''
    def __init__(self):
        self._tokens = {}

    def tokens(self, si, k):
        '''`si` is a stream item and `k` is a key in this feature. The purpose
        of this method is to dereference the token pointers with
        respect to the given stream item. That is, it translates each
        sequence of token pointers to a sequence of `Token`.

        '''

        for tokens in self[k]:
            yield [si.body.sentences[tagid][sid].tokens[tid]
                   for tagid, sid, tid in tokens]

    def offsets(self, si, ty, k):
        for tokens in self.tokens(si, k):
            yield [token.offsets.get(ty) for token in tokens]

    def char_ranges(self, si, k):
        for offsets in self.offsets(si, OffsetType.CHARS, k):
            yield [(o.first, o.first + o.length)
                   for o in offsets if o is not None]

    def xpath_ranges(self, si, k):
        for offsets in self.offsets(si, OffsetType.XPATH_CHARS, k):
            yield list(imap(XpathRange.from_offset, ifilter(None, offsets)))

    def xpath_slices(self, si, k, root=None):
        if root is None:
            root = XpathRange.html_node(si.body.clean_html)
        for xpranges in self.xpath_ranges(si, k):
            yield ' '.join(xp.slice_node(root) for xp in xpranges)

    def to_dict(self):
        # Basically just make sure the tagger id is a Unicode string.
        d = {}
        for fname in self:
            d[fname] = []
            for tokens in self[fname]:
                d[fname].append([(uni(tagid), sid, tid)
                                 for tagid, sid, tid in tokens])
        return d

    @staticmethod
    def from_dict(d):
        ft = FeatureTokens()
        for fname in d:
            for tokens in d[fname]:
                ft[fname].append([(uni(tagid), sid, tid)
                                  for tagid, sid, tid in imap(tuple, tokens)])
        return ft

    # Methods for satisfying `MutableMapping`.

    def __getitem__(self, k):
        return self._tokens.get(uni(k)) or self.__missing__(k)

    def __missing__(self, k):
        v = []
        self[uni(k)] = v
        return v

    def __setitem__(self, k, v): self._tokens[uni(k)] = v
    def __delitem__(self, k): del self._tokens[uni(k)]
    def __len__(self): return len(self._tokens)
    def __iter__(self): return iter(self._tokens)

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, repr(self._tokens))


class FeatureTokensSerializer(object):
    '''Serialization for feature tokens.'''
    def __init__(self):
        raise NotImplementedError()

    dumps = FeatureTokens.to_dict
    constructor = FeatureTokens

    @staticmethod
    def loads(d):
        return FeatureTokens.from_dict(d)


def uni(s):
    if isinstance(s, str):
        return unicode(s, 'utf-8')
    elif isinstance(s, unicode):
        return s
    else:
        raise TypeError(s)
