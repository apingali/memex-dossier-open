# coding=utf-8
'''Tests for phone number extraction and normalization

.. Your use of this software is governed by your license agreement.
   Unpublished Work Copyright 2015 Diffeo, Inc.
'''
from __future__ import absolute_import
import pytest

from memex_dossier.streamcorpus_structured.transform import SelectorType
from memex_dossier.streamcorpus_structured.tests import setup_nltk, configurator

def test_phone_number(configurator):
    """ tests phone number extraction.  Note that the 555 number is not valid
    and the phone number matcher knows this, so it's not included in the results
    """
    phone_text = """some text containing a few formats of phone numbers:
888-867-5309
(888) 867 5309
+1 888 867 5309
+1 555 555 5555"""
    si = setup_nltk(phone_text, run_extractor=True)
    selectors = si.body.selectors['structured-cyber']
    assert len(selectors) == 3
    raw = {selector.raw_selector for selector in selectors}
    canonical = {selector.canonical_selector for selector in selectors}
    types = {selector.selector_type for selector in selectors}
    assert raw == set(['(888) 867 5309', '+1 888 867 5309', '888-867-5309'])
    assert canonical == set([u'+18888675309'])
    assert types == set([SelectorType.PHONE.value])


@pytest.mark.parametrize(
    'text,expected_raw,expected_canonical',
    [
        ('888-867-5309', ['888-867-5309'], [u'+18888675309']),    # random
        # maybe guess country code from page language?
        #(u'tel: 89502154003', ['89502154003'], [u'+89502154003']), # russian cyber example
        (u'\u7535\u8bdd +86-755-26881721', ['+86-755-26881721'], [u'+8675526881721']), # chinese electronics example
        (u'Mobile:\n\n\t    +79033672203', ['+79033672203'], [u'+79033672203']),
    ]
)
def test_phone_numbers_individually(configurator, text, expected_raw, expected_canonical):
    """ tests phone number extraction.
    """
    if isinstance(text, unicode):
        text = text.encode('utf8')
    si = setup_nltk(text, run_extractor=True)
    selectors = si.body.selectors['structured-cyber']
    raw = {selector.raw_selector for selector in selectors}
    canonical = {selector.canonical_selector for selector in selectors}
    types = {selector.selector_type for selector in selectors}
    assert raw == set(expected_raw)
    assert canonical == set(expected_canonical)
    assert types == set([SelectorType.PHONE.value])
