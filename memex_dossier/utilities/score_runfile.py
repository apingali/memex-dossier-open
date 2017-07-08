'''Algorithmically determine soft selector strings.

.. This software is released under an MIT/X11 open source license.
   Copyright 2016 Diffeo, Inc.

'''

import argparse
import csv
import sys
from collections import defaultdict

# I often run this file like:
# python score_runfile.py --truthfile truth.csv  --runfiles ~/runfile.csv --scorer f 

# Tom says: I really want to standardize run files to look liks "A, B, Score".  
#           the following is an annoying hack until then
run_a_col = 0
run_b_col = 2
run_score_col = 4

def test_and_add(set_object, element):
    if element in set_object:
        return True
    else:
        set_object.add(element)
        return False
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--truthfile', help='csv file with truth labels')
    parser.add_argument('--truthtemplate', 
                        default="A,B,score",
                        help='header for the truthfile, it must have columns names "A", "B", and "score"')
    parser.add_argument('--runfiles', 
                        help='comma-separated list of runfiles.  each row in a runfile is a comma-separated list of "id1,id2,score"')
    parser.add_argument('--weights', 
                        default=None, 
                        help='comma separated list of weights for re-ranking a combination of run files')
    parser.add_argument('--onlyone',
                        action='store_true',
                        help='set to "true" if you want to score only the highest ranking "B" id for each "A"')
    parser.add_argument('--outrunfile',
                        default=None,
                        help='output the weighted combination of the input runfiles as a separate runfile')
    parser.add_argument('--scorer',
                        action='append',
                        choices=['precision', 'roc', 'f'],
                        help='which scorer to compute incrementally: precision, roc curve points (true positives/false positives), f-score')

    args = parser.parse_args()

    # read in truth file
    column_names = [x.strip() for x in args.truthtemplate.split(',')]
    A = column_names.index('A')
    B = column_names.index('B')
    score = column_names.index('score')

    truth = {}
    with open(args.truthfile) as truthfile:
        for row in csv.reader(truthfile):
            truth[(row[A].strip(), row[B].strip())] = float(row[score])

    runfiles = [r.strip() for r in args.runfiles.split(',')]
    weights = defaultdict(lambda: 1.0)
    if args.weights:
        weight_list = [float(w) for w in args.weights.split(',')]
        weights.update(zip(runfiles, weight_list))

    scores = defaultdict(lambda: 0)
    for runfile in runfiles:
        with open(runfile) as rf:
            for row in csv.reader(rf):
                try:
                    scores[(row[run_a_col].strip(), row[run_b_col].strip())] += float(row[run_score_col]) * weights[runfile]
                except IndexError:
                    pass

    # print(len(scores))
    filtered_scores = [s for s in scores.items() if s[0] in truth]
    # print(len(filtered_scores))
    sorted_scores = sorted(filtered_scores, key=(lambda x: x[1]), reverse=True)
    # print(len(sorted_scores))

    if args.onlyone:
        seen_scores = set()
        sorted_scores = [s for s in sorted_scores if not test_and_add(seen_scores, s[0][0])]
    # print(len(sorted_scores))

    if args.outrunfile:
        with open(args.outrunfile, 'w') as outfile:
            writer = csv.writer(outfile)
            for score in sorted_scores:
                writer.writerow([score[0][0], score[0][1], score[1]])

    
    if args.scorer:
        total = len(truth)
        total_true = sum((1 if t>0 else 0) for t in truth.values())
        total_false = total - total_true
        if total_true==0:
            raise Exception("Cannot score with no true results")
        if total_false == 0:
            total_false = 1
            
        def output_precision_data(trues, falses):
            return [trues / (trues + falses)]
        def output_roc_data(trues, falses):
            return [trues/total_true, falses/total_false]
        def output_f_data(trues, falses):
            recall = trues/total_true
            precision = trues/(trues + falses)
            if recall:
                return [2 * recall * precision / (recall + precision)]
            else:
                return [0]
        def output_data(trues, falses):
            ret = []
            if 'precision' in args.scorer:
                ret += output_precision_data(trues, falses)
            if 'roc' in args.scorer:
                ret += output_roc_data(trues, falses)
            if 'f' in args.scorer:
                ret += output_f_data(trues, falses)
            return ret

        sofar_true = 0.0
        sofar_false = 0.0

        writer = csv.writer(sys.stdout)

        for record in sorted_scores:
            if truth[record[0]] > 0:
                sofar_true += 1
            else:
                sofar_false += 1
            
            writer.writerow(output_data(sofar_true, sofar_false))
            #print('%s precision at %s' % (float(sum([(1 if truth[x[0]] > 0 else 0) for x in sorted_scores[:k]]))/k, k))
        
        
