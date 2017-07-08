# coding=utf-8
'''tests for structured_features

.. Your use of this software is governed by your license agreement.
   Unpublished Work Copyright 2015 Diffeo, Inc.
'''
from __future__ import absolute_import
import pytest
from md5 import md5

from memex_dossier.streamcorpus_structured.transform import structured_features, SelectorType
from memex_dossier.streamcorpus_structured.structured import find_selectors
from memex_dossier.streamcorpus_structured.tests import setup_nltk, configurator

"""
One test for a string without any features
Others are basic positive tests for current regex extractors.
tests:
    'HEX_VALUE' : hex_value_matcher,
    'BYTE_SEQUENCE' : byte_sequence_matcher,
    'IP_ADDRESS' : ip_address_matcher,
    'IPV6_ADDRESS' : ipv6_address_matcher,
    'MD5' : md5_matcher,
    'email' : email_matcher,
    'URL' : url_matcher,
    'BITTORRENT_URL' : bittorrent_url_matcher,
    'FILE_PATH' : file_path_matcher,
    'CVE_ID' : cve_matcher
    'PHONE': phonenumber_matcher
    'FILE_URI' : file_uri_matcher,
"""

def find_matching_selectors(si, label='structured-cyber', sel_type=None):
    if sel_type:
        sel_fun = lambda sel: sel.selector_type == sel_type
    else:
        sel_fun = lambda sel: True

    return [s for s in si.body.selectors[label] if sel_fun(s)]

@pytest.mark.parametrize("input_text,selector_type,raw_matches", [
    # test hex value regex inside of an html-element like string
    ('0xffff013', SelectorType.HEX_VALUE.value, None),
    # test an escaped byte sequence
    ('\\x00\\xf0\\xa7\\xb0', SelectorType.BYTE_SEQUENCE.value, None),
    # test an ipv4 address
    ('127.0.0.1/seems like an ip', SelectorType.IP_ADDRESS.value, ['127.0.0.1']),
    # test an ipv6 address
    ('2607:f8b0:4000:802::1002/seems like an ip', SelectorType.IPV6_ADDRESS.value,
     ['2607:f8b0:4000:802::1002']),
    # test an md5 hash
    (md5("Look I'm a hashable string").hexdigest(), SelectorType.MD5.value, None),
    #  test an email address
    ('support@diffeo.com', SelectorType.EMAIL.value, None),
    # test a full http url
    ('http://docs.python.org/3.4/library/concurrent.futures.html', SelectorType.URL.value, None),
    # test a standalone domain name
    ('This report can be found at mandiant.com', SelectorType.URL.value, ['mandiant.com']),
    # test a magnet uri (taken from thepiratebay)
    ('magnet:?xt=urn:btih:aca1b874ff9ac0d057218da3f4679b2a2f8c2902&' +
     'dn=Sherlock.S03E03.HDTV.x264-ChameE&' +
     'tr=udp%3A%2F%2Ftracker.openbittorrent.com%3A80&' +
     'tr=udp%3A%2F%2Ftracker.publicbt.com%3A80&' +
     'tr=udp%3A%2F%2Ftracker.istole.it%3A6969&' +
     'tr=udp%3A%2F%2Ftracker.ccc.de%3A80&' +
     'tr=udp%3A%2F%2Fopen.demonii.com%3A1337',
     SelectorType.URL.value, None),
    ('http://foo@bar.com', SelectorType.URL.value, None),
    ('foo@bar.com', SelectorType.EMAIL.value, None),
    ('http://foo@bar.com', SelectorType.EMAIL.value, []),
    ('foo@bar.com', SelectorType.URL.value, []),
    # test a tor url (taken from http://thehiddenwiki.org/)
    ('https://kpvz7ki2v5agwt35.onion/wiki/index.php/', SelectorType.URL.value, None),
    # test a unix-style filepath
    ('/home/swang93/structured-cyber/src/tests/diffeo_cyber/test_regex_extractor.py',
     SelectorType.FILE_PATH.value, None),
    # test a *.exe filename
    ('ctfmon.exe <IP> <port>', SelectorType.FILE_PATH.value, ['ctfmon.exe']),
    # test a cve id
    ('CVE-2013-3333', SelectorType.CVE_ID.value, None),
    (r'C:\some\full\file\path.txt', SelectorType.FILE_PATH.value, None),
    (r'C:\Documents and Settings\testuser\file.txt', SelectorType.FILE_PATH.value, None),
    (r'http://Documents and Settings\testuser\file.txt', SelectorType.FILE_PATH.value, []),
    (r'hxxp://some.url', SelectorType.URL.value, None),
    (r'1[.]2[.]3[.]4', SelectorType.IP_ADDRESS.value, None),
    (r'::', SelectorType.IPV6_ADDRESS.value, []),
    (r'::1', SelectorType.IPV6_ADDRESS.value, None),
])
def test_string_matches_type(configurator, input_text, selector_type, raw_matches):
    if raw_matches is None:
        raw_matches = [input_text]

    with configurator():
        si = setup_nltk(input_text)
        selectors = find_matching_selectors(si, sel_type=selector_type)
        assert len(selectors) == len(raw_matches)
        for sel, raw in zip(selectors, raw_matches):
            assert sel.raw_selector == raw


def test_no_cyber(configurator):
    """
    test a 'normal' string that shouldn't match any cyber structured_features
    """
    with configurator():
        si = setup_nltk('this is a normal string. i am so normal!')
        assert not find_matching_selectors(si)

def test_email(configurator):
    '''Tests email extraction. Particularly, make sure that canonical
    is a lowercased version of email.
    '''

    email_text = 'Testing-Email@mail.com'
    si = setup_nltk(email_text, run_extractor=True)
    selectors = si.body.selectors['structured-cyber']
    raw = {selector.raw_selector for selector in selectors
           if selector.selector_type == 'email'}
    canonical = {selector.canonical_selector for selector in selectors
                 if selector.selector_type == 'email'}
    assert raw == set(['Testing-Email@mail.com'])
    assert canonical == set(['testing-email@mail.com'])

non_ascii = str('some non-ascii characters: ążó')

@pytest.mark.parametrize("input_text,selector_results,text_results", [
    ('1.2.3.4', ['1.2.3.4'], ''),
    (r'888.867-5309 c:\users\user\somefile.txt',
     ['+18888675309', r'c:\users\user\somefile.txt'], ''),
    ('some text that does not have any selectors',
     [], 'some text that does not have any selectors'),
    ('some text with name@email.com in between',
     ['name@email.com'], 'some text with in between'),
    (non_ascii, [], non_ascii.decode('utf-8'))
])
def test_find_selectors(configurator, input_text, selector_results, text_results):
    selectors, remaining = find_selectors(input_text)
    selectors = [sel.canonical_selector for sel in selectors]
    assert all([not isinstance(sel, unicode) for sel in selectors])
    assert set(selectors) == set(selector_results)
    assert remaining == text_results

