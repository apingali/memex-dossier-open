#!/usr/bin/python
# -*- coding: utf-8 -*-
'''Profiles appear in many systems.  A Diffeo system's MediaWiki is
just one source.  The extractors in this module recognize the *slot
values* in an external profile, extract them, and organize them into
an infobox-like data structure that connects string-typed slot names
to lists of Unicode character string values.

.. block-quote:: json

   {slot_name_1: [slot_value_1, ...]}

The slot values come from two sources at the moment: other selector
extractors that have been run on the body content, and the URL string
of the document.


.. Your use of this software is governed by your license agreement.
   Unpublished Work Copyright 2015 Diffeo, Inc.

'''
from __future__ import absolute_import, division, print_function
#import abc
import cbor
from collections import defaultdict
import logging
from urlparse import urlparse
from hashlib import md5

import regex as re
from streamcorpus_pipeline._clean_visible import make_clean_visible
from streamcorpus_pipeline._clean_html import make_clean_html

from memex_dossier.streamcorpus_structured.constants import CANONICAL, RAW
from memex_dossier.streamcorpus_structured.cyber_extractors import phonenumber_matcher, \
    email_matcher, skype_matcher, twitter_matcher
from memex_dossier.streamcorpus_structured.utils import extract_field_from_html, \
    extract_field_from_html_si, extract_name_from_url, \
    generate_fields_from_html_si, \
    extract_name_from_title

from memex_dossier.handles import load_ngrams, prob_username


logger = logging.getLogger(__name__)


class ProfileExtractor(object):
    ## don't actually use abc, because issubclass has issues with
    ## caching weak references
    #__metaclass__ = abc.ABCMeta

    def __init__(self, config=None):
        self.config = config        
        self.char_unigrams, self.char_bigrams = load_ngrams()

    def process(self, si, url_parts):
        slots = self.extract_slots(si, url_parts)
        if slots:
            return {'source_name': self.source_name,
                    'slots': {k: list(v) for k, v in slots.items()}}
        else:
            return None

    #@abc.abstractmethod
    def extract_slots(self, si, url_parts):
        raise NotImplementedError


    def fill_more_slots(self, slots, text, save=False, phone=True,
                        skype=True, twitter=True, si=None):
        clean_html = make_clean_html(text, stream_item=si)
        text = make_clean_visible(clean_html)

        if save:
            open('foobar-%s.txt' % md5(text).hexdigest(), 'wb').write(text)

        for key in ['phone', 'phone_raw', 'Skype', 'Twitter', 'email', 'keywords']:
            if key not in slots:
                slots[key] = set()

        email_matches = list(email_matcher(text))
        if email_matches:
            email_match = email_matches[0][CANONICAL]
            slots['email'].add(email_match)

        if phone:
            phone_matches = list(phonenumber_matcher(text, country='US'))
            for phone_match in phone_matches:
                slots['phone'].add(phone_match[CANONICAL])
                slots['phone_raw'].add(phone_match[RAW])

        if skype:
            skype_matches = list(skype_matcher(text))
            for skype_match in skype_matches:
                slots['Skype'].add(skype_match[CANONICAL])

        if twitter:
            twitter_matches = list(twitter_matcher(text))
            for twitter_match in twitter_matches:
                slots['Twitter'].add(twitter_match[CANONICAL])


        for tok in text.split():  # assume non-CJK
            if prob_username(tok.lower(), self.char_unigrams, self.char_bigrams) > 0.5:
                slots['keywords'].add(tok)

        for key, val in slots.items():
            if not val:
                slots.pop(key)

        return slots




tags = '''(\s*\<[^>]*\>\s*)*\s*'''
com_tradekey_re = re.compile(
ur'''\<div class="contact-info"\>\s*\<ul\>\s*\<li[^>]*\>\s*'''
ur'''(\<span\>Contact Person[^<]*\</span\>\s*\<a[^>]*\>((Mr|Ms|Mrs|Dr).)?\s*(?P<contact_name>[^<]+)\</a\>)?\s*'''
ur'''\<font.*?\</font\>\s*''' + tags +
ur'''(Company[^<]*''' + tags + '''(?P<company_name>[^<]+))?''' + tags +
ur'''(Address[^<]*''' + tags + '''(?P<address>[^<]+))?''' + tags +
ur'''(Zip/Postal[^<]*''' + tags + '''(?P<postal_code>[^<]+))?''' + tags +
ur'''(Telephone[^<]*''' + tags + '''(?P<telephone>[^<]+))?''' + tags +
ur'''(Fax[^<]*''' + tags + '''(?P<fax>[^<]+))?''' + tags +
ur'''(Mobile[^<]*''' + tags + '''(?P<mobile>[^<]+))?''' + tags
, flags=re.UNICODE | re.MULTILINE | re.IGNORECASE)

company_word_re = re.compile(ur'(深圳|市鸿|科技|有限|公司|Company|Co|Limited Liability|Ltd|'
                             ur'Corporation|Corp|Inc|Incorporated|LLC|Limited)'
                             ur'(\.|\,)*',
                             flags=re.UNICODE | re.IGNORECASE)
whitespace_re = re.compile(ur'(\s|\n|\p{Z})+', flags=re.UNICODE | re.IGNORECASE)

def strip_company_words(val):
    val = company_word_re.sub(' ', val)
    val = whitespace_re.sub(' ', val).strip()
    return val

class com_tradekey(ProfileExtractor):

    source_name = 'tradekey.com'

    re_mapping = {
        'contact_name': 'NAME',
        'company_name': 'NAME',
        #'address': 'address',
        #'postal_code': 'postal_code',
        'telephone': 'phone',
        'fax': 'phone',
        'mobile': 'phone',
        }

    def extract_slots(self, si, url_parts):
        slots = defaultdict(list)
        if not extract_name_from_url(url_parts, 0) == 'company':
            # ignore non-profile pages in tradekey.com
            return None

        match = com_tradekey_re.search(si.body.raw)
        if match:
            for group_name, feature_name in self.re_mapping.items():
                val = match.group(group_name).strip()
                try:
                    val = val.decode('utf8')
                except: 
                    val = None
                if val:
                    slots[feature_name].append(val)

                    if group_name == 'company_name':
                        striped_name = strip_company_words(val)
                        if striped_name:
                            slots['NAME'].append(striped_name)

            if slots['phone']:
                normalized_phones = set()
                for raw_string in slots['phone']:
                    if not raw_string.startswith('+'):
                        raw_string = '+' + raw_string
                    for sel in phonenumber_matcher(raw_string):
                        if len(sel[CANONICAL]) > 5:
                            normalized_phones.add(sel[CANONICAL])
                slots['phone'] = list(normalized_phones)

        if slots: return dict(slots)


class com_hongkongcompanylist(ProfileExtractor):

    source_name = 'hongkongcompanylist.com'

    def extract_slots(self, si, url_parts):
        slots = defaultdict(list)

        name1 = extract_field_from_html_si(
            si, '''>Company Name: </div><div class="covalue"><h2>''', '</h2>')
        name2 = extract_field_from_html_si(
            si, '''>Company Name(CN): </div><div class="covalue"><h2>''', '</h2>')

        if name1 and name2:
            slots['NAME'].append(name1)
            slots['NAME'].append(name2)

            striped_name = strip_company_words(name1)
            if striped_name:
                slots['NAME'].append(striped_name)

            striped_name = strip_company_words(name2)
            if striped_name:
                slots['NAME'].append(striped_name)


        if slots: return slots

class com_zhiqiye(ProfileExtractor):

    source_name = 'zhiqiye.com'

    def extract_slots(self, si, url_parts):
        slots = defaultdict(list)
        if not extract_name_from_url(url_parts, 0) == 'company':
            # ignore non-profile pages in zhiqiye.com
            return None

        name = extract_field_from_html_si(si, '''<h3>''', '</h3>')
        if name:
            slots['NAME'].append(name) #.decode('utf8'))
            striped_name = strip_company_words(name)
            if striped_name:
                slots['NAME'].append(striped_name)

        email_text = extract_field_from_html_si(si, '''邮箱''', '''</td>''')
        email_matches = list(email_matcher(email_text))
        if email_matches:
            email_match = email_matches[0][CANONICAL]
            slots['email'].append(email_match)

        phone_text = extract_field_from_html_si(si, '''手机''', '''</td>''')
        phone_matches = list(phonenumber_matcher(phone_text, country='CN'))
        if phone_matches:
            phone_match = phone_matches[0][CANONICAL]
            slots['phone'].append(phone_match)

        if slots: return dict(slots)


class com_pinterest(ProfileExtractor):

    source_name = 'pinterest.com'

    def extract_slots(self, si, url_parts):

        fullname = extract_field_from_html_si(si, '''<h4 class="fullname">''', '''</h4>''')
        fullname = fullname.strip()
        if fullname and len(fullname) > 3:
            return {'username': [fullname]}



url_matchers = {
    'com': {
        'tradekey': com_tradekey,
        'hongkongcompanylist': com_hongkongcompanylist,
        'pinterest': com_pinterest,
        },
    }

def profile_page(si, make_cbor=True):
    '''dispatcher that uses the url_matchers domain name tree above to
    lookup a callable function that returns a list of *linked
    selectors*, which this function serializes using cbor.

    '''
    try:
        url_parts = urlparse(si.abs_url)
    except:
        logger.warn('failed url extraction: %r', si.abs_url, exc_info=True)
        return
    hostname = url_parts.netloc
    root = url_matchers
    for part in reversed(hostname.split('.')):
        if part in root:
            root = root[part]
        else:
            # do not find a parser for this site
            return None
        if type(root) == type and issubclass(root, ProfileExtractor):
            config = {} # could get this from yakonfig?
            extractor = root(config)
            slot_info = extractor.process(si, url_parts)
            if slot_info:
                if make_cbor:
                    data = cbor.dumps(slot_info)
                    # transforms.process_text will put data into
                    # Selector(... metadata=data)
                    return data
                else:
                    return slot_info
            else:
                return
