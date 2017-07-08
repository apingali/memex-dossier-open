'''

.. Your use of this software is governed by your license agreement.
   Unpublished Work Copyright 2015 Diffeo, Inc.

'''
from __future__ import absolute_import, division, print_function
import logging
import urllib

from streamcorpus_pipeline._clean_html import nice_decode, force_unicode

logger = logging.getLogger(__name__)

def extract_field_from_html(raw, field_start_string, field_end_string, si=None):
    field_start = raw.find(field_start_string)
    if field_start == -1: return None
    field_start = field_start + len(field_start_string)

    field_end = raw.find(field_end_string, field_start)
    if field_end == -1: return None

    field = raw[field_start : field_end].strip()
    ufield = nice_decode(field, stream_item=si)
    if not ufield:
        ufield = force_unicode(field)
    return field_start, field_end, ufield

def extract_field_from_html_si(si, field_start_string, field_end_string):
    match = extract_field_from_html(
        si.body.raw, field_start_string, field_end_string, si=si)
    if match:
        field_start, field_end, ufield = match
        return ufield

def generate_fields_from_html_si(si, field_start_string, field_end_string):
    cursor = 0
    while cursor < len(si.body.raw):
        match = extract_field_from_html(
            si.body.raw[cursor:], field_start_string, field_end_string, si=si)
        if match:
            field_start, field_end, ufield = match
            yield ufield
            cursor += field_end
        else:
            break


def extract_name_from_title(si, strip_start=None):
    # parse HTML to get name
    if strip_start is None:
        strip_start = '</title>'
    return extract_field_from_html_si(si, "<title>", strip_start)


def extract_name_from_url(url_parts, path_position):
    path = url_parts.path.lstrip('/').split('/')
    if len(path) < path_position: return None
    username = path[path_position]
    try:
        username = urllib.unquote(username).decode('utf-8')
        return username
    except:
        logger.info('failed to get username from %r',
                    url_parts, exc_info=True)
        return None

