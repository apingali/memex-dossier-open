'''memex_dossier.fc Feature Collections

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

'''
from operator import itemgetter
import sys

from memex_dossier.fc import StringCounter, FeatureCollection, FeatureCollectionChunk


def key_sorted_dict_str(mset):
    return ', '.join(['%s: %d' % (k, mset[k])
                      for k in sorted(mset.iterkeys())])


def repr_feature(feature, max_keys=100, indent=8, lexigraphic=False):
    '''
    generate a pretty-printed string for a feature

    Currently implemented:
      * StringCounter

    @max_keys: truncate long counters

    @indent: indent multi-line displays by this many spaces

    @lexigraphic: instead of sorting counters by count (default), sort
    keys lexigraphically
    '''
    if isinstance(feature, (str, bytes)):
        try:
            ustr = feature.decode('utf8')
            return ustr
        except:
            # failure to decode, not actually utf8, other binary data
            return repr(feature)

    if isinstance(feature, StringCounter):
        return repr_stringcounter(feature, max_keys, indent, lexigraphic)
    elif isinstance(feature, unicode):
        return feature
    else:
        return repr(feature)

    assert False, 'internal logic failure, no branch taken'


def repr_stringcounter(feature, max_keys=100, indent=8, lexigraphic=False):
    if lexigraphic:
        recs = list(feature.iteritems())
        recs.sort(key=itemgetter(0))
        recs = recs[:max_keys]
    else:
        recs = feature.most_common(max_keys)

    longest_key = max(map(len, map(itemgetter(0), recs)))
    if longest_key > 20:
        # count: str
        recs = map(lambda x: '%r: %s' % (x[1], x[0]), recs)
        joiner = '\n' + ' ' * indent
    else:
        # str: count
        recs = map(lambda x: '%s: %r' % (x[0], x[1]), recs)
        joiner = ', '

    len_feature = len(feature)
    if len_feature > max_keys:
        recs.append(' truncated at %d' % max_keys)
    recs.insert(0, ' (%d keys)' % len_feature)
    return joiner.join(recs)


def detailed_view(fc, features_to_show=None, max_keys=100, lexigraphic=False,
                  display_only=True):
    '''
    returns a pretty-printed string describing a
    :class:`memex_dossier.fc.FeatureCollection`.

    @features_to_show: list of features to include, defaults to None
    meaning include all.

    @lexigraphic: passed to repr_feature

    @display_only: if DISPLAY_PREFIX versions of features are present, only show those. (default True)
    '''
    if '#NAME' in fc:
        top_lines = ['NAME: %s' % key_sorted_dict_str( fc.get('#NAME') )]
    elif '#NOM' in fc:
        top_lines = ['NOM: %s' % key_sorted_dict_str( fc.get('#NOM') )]
    elif 'NAME' in fc:
        top_lines = ['NAME: %s' % key_sorted_dict_str( fc.get('NAME') )]
    elif 'NOM' in fc:
        top_lines = ['NOM: %s' % key_sorted_dict_str( fc.get('NOM') )]
    else:
        top_lines = ['no-NAME, no-NOM']
    bottom_lines = []
    feature_names = fc.keys()
    display_only = _check_display_only(display_only, feature_names)
    feature_names.sort()
    hidden_count = 0
    for key in feature_names:
        if display_only and (not key.startswith(FeatureCollection.DISPLAY_PREFIX)):
            hidden_count += 1
            continue
        if key in fc and fc[key]:
            if features_to_show and key not in features_to_show:
                continue
            repr = repr_feature(fc[key], max_keys=max_keys, lexigraphic=lexigraphic)
            if '\n' in repr:
                ## save these for the end
                # use four spaces, smaller than indent=8 above
                bottom_lines.append( '    %s: %s' % (key, repr) )
            else:
                top_lines.append( '    %s: %s' % (key, repr))
    if hidden_count:
        bottom_lines.append('    (and %s hidden features)' % (hidden_count,))
    return '\n'.join(top_lines + bottom_lines)


def _check_display_only(display_only, feature_names):
    if not display_only:
        return False
    for key in feature_names:
        if key.startswith(FeatureCollection.DISPLAY_PREFIX):
            return True
    # no DISPLAY_PREFIX features found, show everything anyway
    return False


def only_specific_multisets(ent, multisets_to_show):
    '''
    returns a pretty-printed string for specific features in a FeatureCollection
    '''
    out_str = []
    for mset_name in multisets_to_show:
        for key, count in ent[mset_name].items():
            out_str.append( '%s - %d: %s' % (mset_name, count, key) )
    return '\n'.join(out_str)


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description='Shows the features for each feature FeatureCollection in a Chunk',
        usage='memex_dossier_feature_dump file.fc')
    parser.add_argument(
        'input', metavar='file.fb', nargs='?', default='-',
        help='defaults to stdin')
    parser.add_argument(
        '-o', '--out', default=None,
        dest='output', help='file to write output to, default stdout')
    parser.add_argument(
        '-f', '--feature', metavar='FEATURE_NAME', default=[],
        dest='features_to_show', action='append', help='only print these features')
    parser.add_argument(
        '--column-view', default=False, action='store_true',
        help='include this multiset in print out')
    parser.add_argument(
        '--max-keys', default=100, type=int,
        help='number of keys to show in counters')
    parser.add_argument(
        '--lexigraphic', default=False, action='store_true',
        help='sort counter keys lexigraphically instead of by count')
    parser.add_argument(
        '--limit', default=None, type=int, metavar='LIMIT',
        help='stop after reading LIMIT records')
    parser.add_argument(
        '--count', action='store_true',
        help='Output a count of the FCs and nothing else.')
    parser.add_argument(
        '--show-hidden', action='store_true',
        help='Show hidden features.')
    args = parser.parse_args()

    if args.output:
        out = open(args.output, 'w')
    else:
        out = sys.stdout

    if args.input == '-':
        i_chunk = FeatureCollectionChunk(file_obj=sys.stdin)
    else:
        ## this allows Chunk to detect .xz and .gpg
        i_chunk = FeatureCollectionChunk(args.input)

    if  args.features_to_show:
        args.features_to_show = set(args.features_to_show)
    if args.count:
        print sum(1 for _ in i_chunk)
        sys.exit(0)

    count = 0
    for fc in i_chunk:
        if args.column_view:
            out_str = only_specific_multisets(fc, args.features_to_show)
        else:
            out_str = detailed_view(
                fc, features_to_show=args.features_to_show,
                max_keys=args.max_keys, lexigraphic=args.lexigraphic,
                display_only=not args.show_hidden)
        if out_str:
            out.write(out_str.encode('utf-8'))
            out.write('\n')
        count += 1
        if (args.limit is not None) and (count >= args.limit):
            break

if __name__ == '__main__':
    main()
