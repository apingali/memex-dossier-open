'''structured - loads basic structured_extractors

.. Your use of this software is governed by your license agreement.
   Unpublished Work Copyright 2015 Diffeo, Inc.
'''
from __future__ import absolute_import
import logging
logger = logging.getLogger(__name__)

from streamcorpus.ttypes import OffsetType
from memex_dossier.streamcorpus_structured import transform

def find_selectors(text, input_selectors=None):
    """function for operating `Selector` extractors on a string.

    This is used by tests and also be other modules that want to
    access selectors without running a transform on a `StreamItem`.

    :param text: the string to search for selectors

    :return: tuple(a list of selectors found in the string,
                   the joined remaining parts of text not covered by any
                   selectors)

    Use this function for to strip selectors out of a short string,
    and return the selector parts, and the remainder separately.  In
    case this string is a composite, or otherwise not verbatim in the
    parsed document, it can first find selectors in the string.  The
    original use case is profile titles - We don't want "Some Guy
    some@guy.com" being split into the boname [Some Guy some guy com],
    so we first pull out the selectors ("Some Guy", "some@guy.com")
    and then the boname will be [Some Guy some@guy.com]

    """

    if not isinstance(text, unicode):
        text = text.decode('utf-8')

    cuts = []
    selectors = []
    if not input_selectors:
        input_selectors = transform.process_text(
            text, transform.structured_features.ORDERED_MATCHERS)
    for selector in input_selectors:
        selectors.append(selector)
        # remember the offsets covered by this string
        offset = selector.offsets[OffsetType.CHARS]
        cuts.append((offset.first, offset.first + offset.length))
    remaining_strings = []
    remove_until_char = 0
    for first, last in sorted(cuts):
        block = text[remove_until_char:first].strip()
        if block:
            remaining_strings.append(block)
        remove_until_char = max(remove_until_char, last)
    block = text[remove_until_char:].strip()
    if block:
        remaining_strings.append(block)

    ## TODO: make this more CJK safe by using the offsets from tokens
    ## to only including whitespace that is in the original.  This
    ## should be a general function streamcorpus_pipeline and should
    ## get reused in several places.  To the extent that the strings
    ## coming out of this function are used for anything, e.g. display
    ## to users or comparison with other strings, they should not have
    ## white space injected into them unless that is what the original
    ## has.
    remaining_text = ' '.join(remaining_strings).strip()
    return selectors, remaining_text
