'''AKAGraph extraction, transformation, and loading utilities.
`get_etl_transforms` is used by the
:func:`memex_dossier.akagraph.core.main`.

.. This software is released under an MIT/X11 open source license.
   Copyright 2015 Diffeo, Inc.

'''
from __future__ import absolute_import, division, print_function
from collections import defaultdict
import csv
import json

from backports import lzma
import cbor
import regex as re

from memex_dossier.streamcorpus_structured.cyber_extractors import phonenumber_matcher


def strip_person_title(name):
    '''remove Mr., Mrs., Ms., from beginning of a string.

    '''
    for prefix in ['Mr. ', 'Mrs. ', 'Ms. ']:
        if name.startswith(prefix):
            return name[len(prefix):]
    return name

def fix_name_order(name):
    parts = name.split(',')
    if len(parts) == 1: return name
    parts.reverse()
    name = ' '.join(parts)
    return name

company_abbr_re = re.compile(ur'(\s|\p{Z}|\p{P})+(co|corp|ltd|limited|inc)(\s|\p{Z}|\p{P}|$)',
                             flags=re.UNICODE | re.IGNORECASE)

whitespace_re = re.compile(ur'\p{Z}+', flags=re.UNICODE | re.IGNORECASE)

places_re = re.compile(ur'\(\s*(shenzhen|hong kong|hk|china|hongkong|h.k.|international|xianggang|group|asia|h.k|shanghai|taiwan|zhonggang|sz|dongguan|industry|beijing|suzhou|shenzhen city|shen zhen|yuandong|xiamen|zhongguo|yatai|xinxing|singapore|kunshan|hongda|zhongshan|tianjin|shouban|shenzhen|network|huizhou|foshan|zerland|xinqi te|xian|wuxi|u.k.|tanghai)\s*\)', 
                       flags = re.IGNORECASE | re.UNICODE)

def strip_company_abbreviations(name):
    name = places_re.sub(' ', name)
    name = company_abbr_re.sub(' ', name)
    name = whitespace_re.sub(' ', name)
    return name.strip()


bad_endings = [
    "00000000", 
    "0000000",
    "11111111",
    "12345678",
    "87654321",
    "8888888",
    "88888888",
]
def is_bad_phone_number(phone):
    for bad_ending in bad_endings:
        if phone[-len(bad_ending):] == bad_ending:
            return True
    return False


def normalize(i_field, o_field, val, hard_selectors):
    if o_field == 'name':
        val = strip_person_title(val)
        val = strip_company_abbreviations(val)

    if i_field in ['representative', 'attn']:
        val = fix_name_order(val)
        # make peoples' names title cased
        val = val.title()

    if i_field == 'title' and val == val.lower():
        # if company name all lower case --> title case
        val = val.title()

    if o_field in hard_selectors:
        val = val.lower()
    if o_field == 'hostname':
        val = val.lstrip('www.')

    return val.strip()

def handle_infot_sz(path, hard_selectors=None):
    return handle_infot(path,
                        {
                            'title': 'name',
                            'post_address': 'postal_address',
                            'representative': 'name',
                            'attn': 'name',
                            'email': 'email',
                            'url': 'hostname',
                            #'phone': 'phone',
                            #'mobile_phone': 'phone',
                            #'fax': 'phone',
                        },
                        (lambda i_rec: 'http://www.yellowpages-china.com/company/%d/'\
                         % int(i_rec['cid'])),
                        
                        hard_selectors)


def handle_infot_cn(path, hard_selectors=None):
    return handle_infot(path,
                        {
                            'title': 'name',
                            'addr': 'postal_address',
                            'email': 'email',
                            'web': 'hostname',
                        },
                        (lambda i_rec: 'file://cngd_2015_un/company/%d/'\
                         % int(i_rec['cid'])),
                        hard_selectors)

def handle_infot(path, field_mapping, to_url, hard_selectors):
    '''Loads data obtained from http://www.yellowpages-china.com/ and
    emits records of the form required by
    :meth:`memex_dossier.akagraph.AKAGraph.add`.

    If `hard_selectors` is provided, it is used to decide when to
    lowercase values.

    '''
    if hard_selectors is None:
        hard_selectors = set()
    if path.endswith('.xz'):
        fopen = lzma.open
    elif path.endswith('.gz'):
        fopen = gzip.open
    else:
        fopen = open
    with fopen(path) as fh:
        phonetypes = set(['phone', 'mobile_phone', 'fax'])
        for i_rec in csv.DictReader(fh):
            o_rec = defaultdict(list)
            for phone_type in phonetypes.intersection(i_rec.keys()): 
                phone_number = None
                for candidate in phonenumber_matcher(
                        i_rec[phone_type], country='CN'):
                    phone_number = candidate['canonical']
                    break
                if phone_number and not is_bad_phone_number(phone_number):
                    o_rec['phone'].append(phone_number)

            for i_field, o_field in field_mapping.iteritems():
                val = i_rec.get(i_field)
                val = val.decode('utf8', errors='ignore')
                if val:
                    val = normalize(i_field, o_field, val, hard_selectors)
                    o_rec[o_field].append(val)
            #print sorted(rec.keys())
            new_o_rec = {}
            for key, vals in o_rec.iteritems():
                # emails and other fields are returned as "email@one; email@two; email@three"
                clean_vals = set()
                for val in vals:
                    clean_vals.update({v.strip() for v in val.split(';') if v.strip()})
                new_o_rec[key] = list(clean_vals)
            new_o_rec['url'] = to_url(i_rec)
            #print json.dumps(new_o_rec, indent=4, sort_keys=True)
            #continue
            yield new_o_rec

# columns from new bigger yellow pages of China
#"cid","web","title","note","attn","job","dept","phone","fax","email","year_of_start","addr","zip","industry","biz_type","biz_kind","repr","numofemp","market","oem","numofdev","sales","exp_vol","imp_vol","certicate","brand","customers","factory_size","reg_date"


def handle_cleansed(path, hard_selectors=None):
    if path.endswith('.xz'):
        fopen = lzma.open
    elif path.endswith('.gz'):
        fopen = gzip.open
    else:
        fopen = open
    with fopen(path) as fh:
        for rec in json.load(fh):
            yield rec


transforms = {
    'infotsz': handle_infot_sz,
    'infotcn': handle_infot_cn,
    'cleansed': handle_cleansed,
}

def get_etl_transforms(data_type):
    if data_type in transforms:
        return transforms[data_type]
    else:
        raise Exception('unknown: %r' % data_type)

