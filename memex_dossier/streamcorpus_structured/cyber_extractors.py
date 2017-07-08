'''cyber_extractors

.. Your use of this software is governed by your license agreement.
   Unpublished Work Copyright 2015 Diffeo, Inc.


streamcorpus_pipeline stage for taking a document and extracting various
 cyber (and other) features into Selectors
'''
from __future__ import absolute_import
import regex as re
import socket
import logging
import phonenumbers

from memex_dossier.streamcorpus_structured.constants import RAW, SPAN, CANONICAL
from memex_dossier.streamcorpus_structured.utils import extract_field_from_html

logger = logging.getLogger(__name__)

special_paths = ['Documents and Settings', 'Program Files', 'Program Files (x86)']

# this is a python raw string, but the \ still needs to be escaped in the regex, hence r'\\' to match \
file_path_str = (r'(\://)?' +   # this line is for later processing to defeat urls
                 r'(?P<path>(?:[A-Za-z]:\\|/)?' +  # leading <drive>:\ or /
                 r'(([A-Za-z0-9\-_]+|%[A-Z_]+%|$[A-Z_]+|' +
                 '|'.join(special_paths) + r')[\\/])*)' +
                 r'(?P<name>[A-Za-z0-9\-_]+|%[A-Z_]+%|$[A-Z_]+)' +
                 r'(?P<ext>\.(?!(com|org|net|edu|gov)\b)[A-Za-z]{2,4})?' +
                 r'\b')
file_path_regex = re.compile(file_path_str)
def file_path_finder(text):
    for candidate in file_path_regex.finditer(text):
        if (not candidate.group().startswith('://')
            and (candidate.group('path') or candidate.group('ext'))):
            ## We want file path to match foo.exe, so we cannot do
            ## this precision booster:
            #has_slash = False
            #if ('/' in candidate.group('path') or '\\' in candidate.group('path')):
            #    has_slash = True
            #if ('/' in candidate.group('ext') or '\\' in candidate.group('ext')):
            #    has_slash = True
            #if not has_slash: continue
            yield candidate

uri_regex = re.compile(
    r'\b'
    r'('
    r'(?P<scheme>https?://|hxxps?://|s?ftp://|file://|magnet:?)' ## URL protocol and colon
    r'|'                          ## or
    r'www[0-9]{0,3}[.]'           ## "www.", "www1.", ... "www999."
    r'|'                          ## or
    r'(?<!@)\b[a-z0-9.\-]+[.]'            ## domain with common TLD:
    r'(com|org|edu|gov|net|uk|ca|de|jp|fr|au|us|ru|ch|it|nl|se|no|es|mil|cn|onion)\b'
    r'|'                          ## or
    r'[a-z0-9.\-]+[.][a-z]{2,4}/' ## looks like domain name followed by a slash
    r')'
    r'('                          ## Zero or more
    r'[^\s()<>\[\]]'                   ## non-space, non-()[]<>
    r'|'                               ## or
    r'\[[^\s()<>\[\]]*\]'              ## balanced brackets
    r'|'                               ## or
    r'\(([^\s()<>]|\([^\s()<>]*\))*\)' ## balanced parens, up to 2 levels
    r')*'
    r'('                          ## End with
    r'\[[^\s()<>\[\]]*\]'              ## balanced brackets
    r'|'                               ## or
    r'\(([^\s()<>]|\([^\s()<>]*\))*\)' ## balanced parens, up to 2 levels
    r'|'                               ## or
    r'(?<=[^\s`!()\[\]{};:\'".,<>?])'  ## not a space or one of these punct chars
    r')'
    , re.IGNORECASE)

cve_regex = re.compile(
    r'\bCVE-\d{4}-\d{4,}\b'
)
hex_value_regex = re.compile(
    r'\b0x'
    r'(?P<value>[a-fA-F\d]+)'
    r'\b'
)
byte_sequence_regex = re.compile(
    r'(\\x[a-fA-F\d]{2})+\b'
)
ip_address_regex = re.compile(
    r'\b'
    r'(?P<ip_address>(\d{1,3}[[]?\.[]]?){3}\d{1,3})'
    r'\b'
)
ipv6_address_regex = re.compile(
    ## for simplicity, matches a superset of valid addresses
    ## validate matches using socket library
    r'(\b([a-fA-F\d]{1,4}:){1,6}|:(?=:))' ## sequence of '[abcd]:' groups, or single ':' if followed by ':'
    r'[a-fA-F\d]{0,4}'                    ## optional 'abcd' group
    r'((:[a-fA-F\d]{1,4}){1,6}\b|(?<=:):' ## sequence of ':[abcd]' groups, or single ':' if preceded by ':'...
    r'|(?=:\d{1,3}(\.\d{1,3}){3}\b))'     ## ...or nothing if followed by ':[ipv4 address]'
    r'(?!\d{0,3}(\.\d{1,3}){3})'          ## make sure we didn't eat part of an ipv4 address
    r'(:\d{1,3}(\.\d{1,3}){3}\b)?'        ## optional embedded ipv4 address
)

def ipv6_address_matcher(text):
    for match in ipv6_address_regex.finditer(text):
        raw = match.group()
        try:
            internal = socket.inet_pton(socket.AF_INET6, raw)
            normalized = socket.inet_ntop(socket.AF_INET6, internal)
            if raw != '::':
                yield {RAW: raw, CANONICAL: normalized, SPAN: match.span()}
        except:
            pass

md5_regex = re.compile(
    r'\b[a-fA-F\d]{32}\b'
)
email_regex = re.compile(
    r'(?<!://)\b'
    r'(?P<username>[A-Za-z0-9.\-\+]+)'
    r'@'
    r'(?P<domain>[A-Za-z0-9.\-]+[.][A-Za-z]{2,4})'
    r'\b'
)

def email_matcher(text):
    for match in email_regex.finditer(text):
        raw = match.group()

        # Email may be in punycode format, but that is okay. None
        # of the punycode semantics rely on case. A bigger concern
        # is that email technically can be case-sensitive, depending
        # on the host. But no major host actually permits case-sensitive
        # emails. We weighed this with the fact that emails in source_ids
        # in mediawiki can have the first character capitalized, and this
        # fixes that.
        canonical = raw.lower()

        yield {RAW: raw, CANONICAL: canonical, SPAN: match.span()}


english = u'tel|telephone|phone|mobile|cell'
russian = u'\u0442\u0435\u043b\u0435\u0444\u043e\u043d|\u0444\u043e\u043d\u0430'
chinese = (u'\u7535\u8bdd|\u4f20\u771f\u673a|\u7535\u8bdd|\u7535\u8bdd|\u97f3\u7d20')

phone_regex = re.compile(
    ur'(' + ur'|'.join([english, russian, chinese]) + ur')'
    ur'(\s|\n|\p{Z}|:)*'
    ur'(?P<number>\+?[\p{Z}\p{N}-]{7,20})',
    flags = re.UNICODE | re.MULTILINE | re.IGNORECASE)

def format_number(phone_number_or_match):
    number = phone_number_or_match
    if hasattr(number, 'number'):
        number = number.number
    return phonenumbers.format_number(number,
                                      phonenumbers.PhoneNumberFormat.E164)

def phonenumber_matcher(text, country=None):
    candidates = []
    try:
        # international phone numbers
        candidates += list(phonenumbers.PhoneNumberMatcher(text, None))
    except UnicodeDecodeError:
        pass

    try:
        # US phone numbers
        candidates += list(phonenumbers.PhoneNumberMatcher(text, 'US'))
    except UnicodeDecodeError:
        pass

    if country is not None:
        try:
            candidates += list(phonenumbers.PhoneNumberMatcher(text, country))
        except UnicodeDecodeError:
            pass

    for match in phone_regex.finditer(text):
        raw_string = match.group('number')
        try:
            if raw_string.startswith('+'):
                number = phonenumbers.parse(raw_string, None)
            else:
                # guess country code from page language?
                continue
                #number = phonenumbers.parse(raw_string, None)
            candidates.append(number)
        except:
            continue


    prior = set()
    # take the longest canonical string derived for each raw span that matches
    for match in sorted(candidates, key=lambda x: len(format_number(x)), reverse=True):
        if not hasattr(match, 'start'):
            # expect to get it with another one
            continue
        span = match.start, match.start + len(match.raw_string)
        if span not in prior:
            prior.add(span)
            yield {SPAN: span, RAW: match.raw_string, CANONICAL: format_number(match)}


# Skype Username Restrictions: A Skype username cannot be shorter than
# six characters or longer than 32. It can contain both letters and
# numbers, but must start with a letter; accented characters are not
# allowed. The only punctuation marks you can use are commas, dashes,
# periods and underscores.

skype_re = re.compile(ur'skype(\s|ID|handle|name|\<b\>|\:)*'
                      ur'(?P<skype>[a-zA-Z][a-zA-Z0-9,-._]{5,31})',
                      flags = re.UNICODE | re.IGNORECASE)

def skype_matcher(text):
    for match in skype_re.finditer(text):
        sub = match.span('skype')
        raw_string = text[sub[0]:sub[1]]
        yield {SPAN: sub,
               RAW: raw_string,
               CANONICAL: raw_string}


# similar rules to skype names...

twitter_re = re.compile(ur'twitter(\s|ID|handle|name|\<b\>|\:)*'
                        ur'(?P<twitter>[a-zA-Z][a-zA-Z0-9,-._]{3,31})',
                        flags = re.UNICODE | re.IGNORECASE)

def twitter_matcher(text):
    for match in twitter_re.finditer(text):
        sub = match.span('twitter')
        raw_string = text[sub[0]:sub[1]]
        yield {SPAN: sub,
               RAW: raw_string,
               CANONICAL: raw_string}
