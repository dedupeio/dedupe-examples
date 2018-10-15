import csv
import collections
import itertools
import os

def evaluateDuplicates(found_dupes, true_dupes):
    true_positives = found_dupes.intersection(true_dupes)
    false_positives = found_dupes.difference(true_dupes)
    uncovered_dupes = true_dupes.difference(found_dupes)

    print('found duplicate')
    print(len(found_dupes))

    print('precision')
    print(1 - len(false_positives) / float(len(found_dupes)))

    print('recall')
    print(len(true_positives) / float(len(true_dupes)))


def linkPairs(filename, rowname) :
    link_d = {}

    with open(filename) as f:
        reader = csv.DictReader(f, delimiter=',', quotechar='"')
        for i, row in enumerate(reader):
            source_file, link_id = row['source file'], row[rowname]
            if link_id:
                if link_id not in link_d:
                    link_d[link_id] = collections.defaultdict(list)

                link_d[link_id][source_file].append(i)

    link_s = set()

    for members in link_d.values():
        for pair in itertools.product(*members.values()):
            link_s.add(frozenset(pair))

    return link_s

clusters = 'data_matching_output.csv'

true_dupes = linkPairs(clusters, 'unique_id')
test_dupes = linkPairs(clusters, 'Cluster ID')

evaluateDuplicates(test_dupes, true_dupes)

