import csv
import collections
import itertools

def evaluateDuplicates(found_dupes, true_dupes):
    true_positives = found_dupes.intersection(true_dupes)
    false_positives = found_dupes.difference(true_dupes)
    uncovered_dupes = true_dupes.difference(found_dupes)

    print 'found duplicate'
    print len(found_dupes)

    print len(true_dupes)

    print 'precision'
    print 1 - len(false_positives) / float(len(found_dupes))

    print 'recall'
    print len(true_positives) / float(len(true_dupes))


def dupePairs(filename, colname) :
    dupe_d = collections.defaultdict(list)

    with open(filename) as f:
        reader = csv.DictReader(f, delimiter=',', quotechar='"')
        for row in reader:
            dupe_d[row[colname]].append(row['person_id'])

    if 'x' in dupe_d :
        del dupe_d['x']

    dupe_s = set([])
    for (unique_id, cluster) in dupe_d.iteritems():
        if len(cluster) > 1:
            for pair in itertools.combinations(cluster, 2):
                dupe_s.add(frozenset(pair))

    return dupe_s

dedupe_clusters = 'patstat_output.csv'
manual_clusters = 'patstat_reference.csv'

test_dupes = dupePairs(dedupe_clusters, 'Cluster ID')
true_dupes = dupePairs(manual_clusters, 'leuven_id')

evaluateDuplicates(test_dupes, true_dupes)

