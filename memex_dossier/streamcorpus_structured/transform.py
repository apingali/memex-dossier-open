'''This provides the primary external interface to
streamcorpus-structured.  It is a streamcorpus_pipeline transform that
is entry point injected into streamcorpus_pipeline.stages via this
package's setup.py

.. Your use of this software is governed by your license agreement.
   Unpublished Work Copyright 2015 Diffeo, Inc.

'''
from __future__ import absolute_import
import logging
from operator import itemgetter
from enum import Enum

from streamcorpus_pipeline.stages import IncrementalTransform
from streamcorpus_pipeline._exceptions import ConfigurationError
from streamcorpus import Selector, Offset, OffsetType, StreamItem

from memex_dossier.streamcorpus_structured import cyber_extractors, geo_extractors, \
    page_extractors
from memex_dossier.streamcorpus_structured.constants import RAW, SPAN, CANONICAL

logger = logging.getLogger(__name__)

# TODO: improve tests to

def process_text(stream_item_or_text, ordered_matchers):
    """This is the primary way of calling all of the matchers.  It is
    called from inside the IncrementalTransform below.

    :param stream_item_or_text: Can be either a string, a unicode, or
    a :class:`StreamItem` instance.  Some extractors require a
    `StreamItem` and others can operate on only a string.  If it lacks
    :attr:`body.clean_visible` then no `Selector` instances will be
    generated.

    :param ordered_matchers: a list of tuples(name, function) where
                     each function yields with re.match objects or
                     dicts from which a Selector is made

    :yields: :class:`~streamcorpus.Selector` instances

    """
    if isinstance(stream_item_or_text, basestring):
        stream_item = None
        if not isinstance(stream_item_or_text, unicode):
            text = stream_item_or_text.decode('utf-8')
        else:
            text = stream_item_or_text
    else:
        assert isinstance(stream_item_or_text, StreamItem)
        stream_item = stream_item_or_text
        if not hasattr(stream_item, 'body'):
            return
        elif stream_item.body.clean_visible:
            text = stream_item.body.clean_visible.decode('utf-8')
        else:
            text = stream_item.body.raw

    for name, matcher in ordered_matchers:
        if name.endswith('_PAGE'):
            if stream_item is not None:
                selector_metadata = matcher(stream_item)
                if selector_metadata is None: continue
                yield Selector(selector_type=name,
                               metadata=selector_metadata)
            continue
        # non-page extractors
        try:
            for match in matcher(text):
                if isinstance(match, dict):
                    raw = match[RAW]
                    canonical = match.get(CANONICAL, raw)
                    span = match.get(SPAN)
                else:
                    # match is an re.match instance
                    raw = match.group()
                    canonical = raw
                    span = match.span()
                if not name.endswith('JSON'):
                    canonical = canonical.replace(u' ', u'_')
                o = Offset(type=OffsetType.CHARS,
                           content_form='clean_visible',
                           first=span[0], length=span[1] - span[0])
                yield Selector(selector_type=name,
                               raw_selector=raw.encode('utf-8'),
                               canonical_selector=canonical.encode('utf-8'),
                               offsets={OffsetType.CHARS: o})
        except Exception as e:  # don't let one matcher's error kill the rest
            logger.warn('matcher %s threw an error %s', name, e, exc_info=True)


class SelectorType(Enum):
    '''This enumerates all of the Selectors for which we have matchers.

    This is a break from the original style of
    streamcorpus/if/*.thrift in that we define the enum here in code
    rather than in the Thrift IDL.

    SelectorTypes that end in the string "_PAGE" expect to receive a
    StreamItem as input.

    '''
    # NB: ending in "_JSON" is special: canonical_selector is JSON.
    GEOJSON = 'GEOJSON'

    # NB: ending in "_PAGE" is special, see above and `process_text`
    PROFILE_PAGE = 'PROFILE_PAGE'

    GEOHASH = 'GEOHASH'
    PHONE = 'PHONE'
    HEX_VALUE = 'HEX_VALUE'
    BYTE_SEQUENCE = 'BYTE_SEQUENCE'
    IP_ADDRESS = 'IP_ADDRESS'
    IPV6_ADDRESS = 'IPV6_ADDRESS'
    MD5 = 'MD5'
    EMAIL = 'email'
    MAGIC_VALUE = 'MAGIC_VALUE'
    HTTP_REQUEST = 'HTTP_REQUEST'
    URL = 'URL'
    FILE_PATH = 'FILE_PATH'
    CVE_ID = 'CVE_ID'
    SKYPE = 'SKYPE'
    TOPIC = 'TOPIC'


class structured_features(IncrementalTransform):
    """
    Uses regex or custom matchers to find selectors in clean_visible
    and adds these selectors, offsets, types, and canonical strings to
    ContentItems.selectors

    .. code-block:: yaml

        structured_features:
          tagger_id_to_create: cyber
          ordered_matchers: [SKYPE, email, PROFILE_PAGE]

    'tagger_id_to_create' specifies the name to give the list of found selectors
    for the example above, that would be selectors['cyber']

    """

    config_name = 'structured_features'
    default_config =  {
        'tagger_id_to_create': 'structured',
    }

    # this ordering determines the match precedence in
    # treelab.pipeline.utils.get_non_overlapping_sorted_selectors
    ORDERED_MATCHERS = [
        (SelectorType.GEOHASH.value, geo_extractors.locations_geohash),
        (SelectorType.GEOJSON.value, geo_extractors.locations_geojson),
        (SelectorType.PHONE.value, cyber_extractors.phonenumber_matcher),
        (SelectorType.HEX_VALUE.value, cyber_extractors.hex_value_regex.finditer),
        (SelectorType.BYTE_SEQUENCE.value, cyber_extractors.byte_sequence_regex.finditer),
        (SelectorType.IP_ADDRESS.value, cyber_extractors.ip_address_regex.finditer),
        (SelectorType.IPV6_ADDRESS.value, cyber_extractors.ipv6_address_matcher),
        (SelectorType.MD5.value, cyber_extractors.md5_regex.finditer),
        (SelectorType.EMAIL.value, cyber_extractors.email_matcher),
        # see diffeo-cyber if these become important
        #SelectorType.MAGIC_VALUE.value, magic_value_matcher),
        #SelectorType.HTTP_REQUEST.value, http_request_matcher),
        (SelectorType.URL.value, cyber_extractors.uri_regex.finditer),
        (SelectorType.FILE_PATH.value, cyber_extractors.file_path_finder),
        (SelectorType.CVE_ID.value, cyber_extractors.cve_regex.finditer),
        (SelectorType.SKYPE.value, cyber_extractors.skype_matcher),

        # This goes after all previous, so it can use their output.
        (SelectorType.PROFILE_PAGE.value, page_extractors.profile_page),
    ]

    MATCHER_PRIORITY = map(itemgetter(0), ORDERED_MATCHERS)
    ALL_MATCHERS = dict(ORDERED_MATCHERS)

    SELECTORS_WITH_OFFSETS = set(MATCHER_PRIORITY)
    SELECTORS_WITH_OFFSETS.remove('PROFILE_PAGE')

    def __init__(self, config):
        super(structured_features, self).__init__(config)
        self.tagger_id_to_create = self.config.get('tagger_id_to_create')
        if 'ordered_matchers' in config:
            self.ordered_matchers = []
            for name in config['ordered_matchers']:
                if name not in SelectorType:
                    raise ConfigurationError('%r not in %r' % (name, self.ALL_MATCHERS))
                self.ordered_matchers.append((name, self.ALL_MATCHERS[name]))
        else:
            self.ordered_matchers = list(self.ORDERED_MATCHERS)
        if not self.tagger_id_to_create:
            raise ConfigurationError('Must specify a tagger_id_to_create')

    def process_item(self, stream_item, context=None):

        stream_item.body.selectors[self.tagger_id_to_create] = []
        for sel in process_text(stream_item, self.ordered_matchers):
                    stream_item.body.selectors[self.tagger_id_to_create].append(sel)

        return stream_item

    __call__ = process_item
