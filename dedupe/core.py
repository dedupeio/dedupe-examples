#!/usr/bin/python
# -*- coding: utf-8 -*-


import collections
import random
import json
import itertools
import logging
from itertools import count
import warnings
from itertools import count, izip_longest, chain, izip, repeat
import warnings
from multiprocessing import Pool

import numpy

import lr
from dedupe.distance.affinegap import normalizedAffineGapDistance as stringDistance

def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)

def randomPairs(n_records, sample_size, zero_indexed=True):
    """
    Return random combinations of indices for a square matrix of size
    n records
    """

    if n_records < 2 :
        raise ValueError("Needs at least two records")
    n = n_records * (n_records - 1) / 2

    if sample_size >= n:
        warnings.warn("Requested sample of size %d, only returning %d possible pairs" % (sample_size, n))

        random_indices = numpy.arange(n)
        numpy.random.shuffle(random_indices)
    else:
        try:
            random_indices = numpy.array(random.sample(xrange(n), sample_size))
        except OverflowError:
            # If the population is very large relative to the sample
            # size than we'll get very few duplicates by chance
            logging.warning("There may be duplicates in the sample")
            sample = numpy.array([random.sample(xrange(n_records), 2)
                                  for _ in xrange(sample_size)])
            return numpy.sort(sample, axis=1)



    b = 1 - 2 * n_records

    x = numpy.trunc((-b - numpy.sqrt(b ** 2 - 8 * random_indices)) / 2)
    y = random_indices + x * (b + x + 2) / 2 + 1

    if not zero_indexed:
        x += 1
        y += 1

    return numpy.column_stack((x, y)).astype(int)



def trainModel(training_data, data_model, alpha=.001):
    """
    Use logistic regression to train weights for all fields in the data model
    """
    
    labels = training_data['label']
    examples = training_data['distances']

    (weight, bias) = lr.lr(labels, examples, alpha)

    for i, name in enumerate(data_model['fields']) :
        data_model['fields'][name]['weight'] = float(weight[i])

    data_model['bias'] = bias

    return data_model


def fieldDistances(record_pairs, data_model):
    fields = data_model['fields']

    field_comparators = [(field, v['comparator'])
                         for field, v in fields.items()
                         if v['type'] not in ('Missing Data',
                                              'Interaction',
                                              'Higher Categories')]

    
    missing_field_indices = [i for i, (field, v) 
                             in enumerate(fields.items())
                             if 'Has Missing' in v and v['Has Missing']]

    field_names = fields.keys()
  
    interactions = []
    categorical_indices = []

    for field in fields :
        if fields[field]['type'] == 'Interaction' :
            interaction_indices = []
            for interaction_field in fields[field]['Interaction Fields'] :
                interaction_indices.append(field_names.index(interaction_field))
            interactions.append(interaction_indices)
        if fields[field]['type'] in ('Source', 'Categorical') :
            categorical_indices.append((field_names.index(field), 
                                        fields[field]['comparator'].length))


    field_distances = numpy.fromiter((compare(record_pair[0][field],
                                              record_pair[1][field]) 
                                      for record_pair in record_pairs 
                                      for field, compare in field_comparators
                                      if record_pair), 
                                     'f4')
    field_distances = field_distances.reshape(-1,len(field_comparators))

    for cat_index, length in categorical_indices :
        different_sources = field_distances[:, cat_index][...,None] == numpy.arange(2, length)[None,...]


        field_distances[:, cat_index][field_distances[:, cat_index] > 1] = 0
        field_distances = numpy.concatenate((field_distances,
                                             different_sources.astype(float)),
                                            axis=1)


    interaction_distances = numpy.empty((field_distances.shape[0],
                                         len(interactions)))

    for i, interaction in enumerate(interactions) :
        a = numpy.prod(field_distances[...,interaction], axis=1)
        interaction_distances[...,i] = a
       
    field_distances = numpy.concatenate((field_distances,
                                         interaction_distances),
                                        axis=1)

        

    missing_data = numpy.isnan(field_distances)

    field_distances[missing_data] = 0

    missing_indicators = 1-missing_data[:,missing_field_indices]
    

    field_distances = numpy.concatenate((field_distances,
                                         missing_indicators),
                                        axis=1)


    return field_distances

def _fieldDistances(args) :
    record_pairs, data_model = args
    return fieldDistances(record_pairs, data_model)
    

def scorePairs(field_distances, data_model):
    fields = data_model['fields']

    field_weights = [fields[name]['weight'] for name in fields]
    bias = data_model['bias']

    scores = numpy.dot(field_distances, field_weights)

    scores = numpy.exp(scores + bias) / (1 + numpy.exp(scores + bias))

    return scores

def _scorePairs(args) :
    field_distance_chunk, data_model = args
    return scorePairs(field_distance_chunk, data_model)



def scoreDuplicates(ids, records, id_type, data_model, threshold=None, num_processes=1):
    pool = Pool(processes=num_processes)

    score_dtype = [('pairs', id_type, 2), ('score', 'f4', 1)]

    chunk_size = 10000
    
    record_chunks = grouper(records, chunk_size)

    field_distances = pool.imap(_fieldDistances, 
                                izip(record_chunks, 
                                     repeat(data_model)))

    dupe_scores = pool.imap(_scorePairs,
                            izip(field_distances, 
                                 repeat(data_model)))


    dupe_scores = chain.from_iterable(dupe_scores)  

    scored_pairs = ((pair_id, score) 
                    for pair_id, score in izip(ids, dupe_scores) 
                    if score > threshold)

    scored_pairs =  numpy.fromiter(scored_pairs,
                                   dtype=score_dtype)


    logging.info('all scores %d' % scored_pairs.shape)
    scored_pairs = numpy.unique(scored_pairs)
    logging.info('unique scores %d' % scored_pairs.shape)

    return scored_pairs

def blockedPairs(blocks) :
    for block in blocks :

        block_pairs = itertools.combinations(block, 2)

        for pair in block_pairs :
            yield pair

def split(iterable):
    it = iter(iterable)
    q = [collections.deque([x]) for x in it.next()] 
    def proj(qi):
        while True:
            if not qi:
                for qj, xj in zip(q, it.next()):
                    qj.append(xj)
            yield qi.popleft()
    for qi in q:
        yield proj(qi)

class frozendict(dict):
    def _blocked_attribute(obj):
        raise AttributeError, "A frozendict cannot be modified."

#    _blocked_attribute = property(_blocked_attribute)

#    __delitem__ = __setitem__ = clear = _blocked_attribute
#    pop = popitem = setdefault = update = _blocked_attribute
    def __repr__(self):
        return "frozendict(%s)" % dict.__repr__(self)

