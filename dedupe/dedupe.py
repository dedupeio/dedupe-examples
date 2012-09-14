#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
import random

import core
import training_sample
import crossvalidation
from predicates import *
import blocking
import clustering
import numpy

import types

def sampleDict(d, sample_size):

    if len(d) <= sample_size:
        return d

    sample_keys = random.sample(d.keys(), sample_size)
    return dict((k, d[k]) for k in d.keys() if k in sample_keys)


class Dedupe:
    """
    Public methods
    __init__
    initializeTraining
    train
    blockingFunction
    clusterDuplicates
    writeTraining
    writeSettings
    """
    def __init__(self, init=None):
        """
        Load or initialize a data model

        Keyword arguments:
        init -- a field definition or a file location for a settings
                file

        a field definition is a dictionary where the keys are the fields
        that will be used for training a model and the values are the
        field specification

        field types include
        - String
        - Interaction

        a 'String' type field must have as its key a name of a field
        as it appears in the data dictionary and a type declaration
        ex. {'Phone': {type: 'String'}}

        an 'Interaction' type field should have as its keys the names
        of the fields involved in the interaction, and must include
        a type declaration and a sequence of the interacting fields
        as they appear in the data dictionary

        ex. {'name:city' : {'type': 'Interaction',
                            'interaction-terms': ['name', 'city']}}

        Longer example of a field definition:
        fields = {'name':       {'type': 'String'},
                  'address':    {'type': 'String'},
                  'city':       {'type': 'String'},
                  'cuisine':    {'type': 'String'},
                  'name:city' : {'type': 'Interaction',
                                 'interaction-terms': ['name', 'city']}
                  }

        settings files are typically generated by saving the settings
        learned in a previous session. If you need details for this
        file see the method writeSettings.
         
        """
        if init.__class__ is dict and init:
            self._initializeSettings(init)
        elif init.__class__ is str and init:
            self.readSettings(init)            
        elif init:
            raise ValueError("Incorrect Input Type: must supply either "
                             "a field definition or a settings file."
                             )
        else:
            raise ValueError("No Input: must supply either "
                             "a field definition or a settings file."
                             )


        n_fields = len(self.data_model['fields'])
        field_dtype = [('names', 'a20', n_fields),
                       ('values', 'f4', n_fields)]
        training_dtype = [('label', 'i4'),
                          ('field_distances', field_dtype)]

        self.training_data = numpy.zeros(0, dtype=training_dtype)

    def loadData(self, data_d, sample_size=None):
        self.record_distance = None
        self.data_d = None
        pass
            
    def _initializeSettings(self, fields):
        data_model = {}
        data_model['fields'] = {}

        for (k, v) in fields.iteritems():
            if v.__class__ is not dict:
                raise ValueError("Incorrect field specification: "
                                 "field specifications are dictionaries "
                                 "that must include a type definition, "
                                 "ex. {'Phone': {type: 'String'}}"
                                 )
            elif 'type' not in v:
                raise ValueError("Incorrect field specification: "
                                 "field specifications are dictionaries "
                                 "that must include a type definition, "
                                 "ex. {'Phone': {type: 'String'}}"
                                 )
            elif v['type'] not in ['String', 'Interaction']:
                raise ValueError("Incorrect field specification: "
                                 "field specifications are dictionaries "
                                 "that must include a type definition, "
                                 "ex. {'Phone': {type: 'String'}}"
                                 )
            elif v['type'] == 'Interaction' and ('interaction_terms'
                                                 not in v):
                raise ValueError("Interaction terms not set: "
                                 "Interaction types must include a "
                                 "type declaration and a sequence of "
                                 "the interacting fields as they appear "
                                 "in the data dictionary. ex. {'name:city' "
                                 ": {'type': 'Interaction', "
                                 "'interaction-terms': ['name', 'city']}}"
                                 )
                
            
            v.update({'weight': 0})
            data_model['fields'][k] = v

        data_model['bias'] = 0
        self.data_model = data_model
        self.alpha = 0
        self.predicates = None

    def initializeTraining(self, training_file=None) :
        """
        
        """
        n_fields = len(self.data_model['fields'])

        field_dtype = [('names', 'a20', n_fields),
                       ('values', 'f4', n_fields)]
        training_dtype = [('label', 'i4'),
                          ('field_distances', field_dtype)]

        self.training_data = numpy.zeros(0, dtype=training_dtype)

        if training_file :
            (self.training_pairs,
             self.training_data) = self.readTraining(training_source,
                                                     self.training_data)                    
                


    def train(self, data_d, training_source=None) :
        """
        Learn field weights and blocking predicate from file of
        labeled examples or round of interactive labeling

        Keyword arguments:
        data_d -- a dictionary of records
        training_source -- either a path to a file of labeled examples or
                           a labeling function

                           
        

        In the dictionary of records, the keys are unique identifiers
        for each record, the values are a dictionary where the keys
        are the names of the record field and values are the record
        values.

        For Example,
        {
         854: {'city': 'san francisco',
               'address': '300 de haro st.',
               'name': "sally's cafe & bakery",
               'cuisine': 'american'},
         855: {'city': 'san francisco',
               'address': '1328 18th st.',
               'name': 'san francisco bbq',
               'cuisine': 'thai'}
         }

        The labeling function will be used to do active learning. The
        function will be supplied a list of examples that the learner
        is the most 'curious' about, that is examples where we are most
        uncertain about how they should be labeled. The labeling function
        will label these, and based upon what we learn from these
        examples, the labeling function will be supplied with new
        examples that the learner is now most curious about.  This will
        continue until the labeling function sends a message that we
        it is done labeling.
            
        The labeling function must be a function that takes two
        arguments.  The first argument is a sequence of pairs of
        records. The second argument is the data model.

        The labeling function must return two outputs. The function
        must return a dictionary of labeled pairs and a finished flag.

        The dictionary of labeled pairs must have two keys, 1 and 0,
        corresponding to record pairs that are duplicates or
        nonduplicates respectively. The values of the dictionary must
        be a sequence of records pairs, like the sequence that was
        passed in.

        The 'finished' flag should take the value False for active
        learning to continue, and the value True to stop active learning.

        i.e.

        labelFunction(record_pairs, data_model) :
            ...
            return (labeled_pairs, finished)

        For a working example, see consoleLabel in training_sample

        Labeled example files are typically generated by saving the
        examples labeled in a previous session. If you need details
        for this file see the method writeTraining.
        """
        if (training_source.__class__ is not str
            and not isinstance(training_source, types.FunctionType)):
            raise ValueError

        self.data_d = sampleDict(data_d, 700)

        if training_source.__class__ is str:
            if not hasattr(self, 'training_data'):
                self.initializeTraining(training_source)
            else:
                (self.training_pairs,
                 self.training_data) = self.readTraining(training_source,
                                                         training_data)
        elif isinstance(training_source, types.FunctionType) :
            if not hasattr(self, 'training_data'):
                self.initializeTraining()
            (self.training_data,
            self.training_pairs,
            self.data_model) = training_sample.activeLearning(self.data_d,
                                                              self.data_model,
                                                              training_source,
                                                              self.training_data)




    def blockingFunction(self):
        """
        Returns a function that takes in a record dictionary and
        returns a list of blocking keys for the record. We will
        learn the best blocking predicates if we don't have them already.

        We'll allow for predicates to be passed
        """
        if not self.predicates:
            self.predicates = self._learnBlocking(self.data_d)

        bF = blocking.createBlockingFunction(self.predicates)

        return bF

    def duplicateClusters(self,
                          blocked_data,     
                          clustering_algorithm=clustering.hierarchical.cluster,
                          pairwise_threshold = .5,
                          cluster_threshold = .5,
                          **args):

        candidates = blocking.mergeBlocks(blocked_data)
        self.dupes = core.scoreDuplicates(candidates, 
                                          self.data_model,
                                          pairwise_threshold)
        clusters = clustering_algorithm(self.dupes, cluster_threshold, **args)

        return clusters
        
    def _findAlpha(self):
        pass

    def _train(self):
        self.findAlpha()
        self.data_model = core.trainModel(self.training_data,
                                          self.data_model,
                                          self.alpha)
        self.printLearnedWeights()


    def _activeLearning(self,
                        data_d,
                        labelingFunction,
                        numTrainingPairs=30,
                        ):

        (self.training_data,
         self.training_pairs,
         self.data_model) = training_sample.activeLearning(sampleDict(data_d,
                                                                      700),
                                                           self.data_model,
                                                           labelingFunction,
                                                           self.training_data)

    def _learnBlocking(self, data_d):
        confident_nonduplicates = blocking.semiSupervisedNonDuplicates(sampleDict(data_d, 700),
                                                                           self.data_model)

        self.training_pairs[0].extend(confident_nonduplicates)

        predicate_functions = (wholeFieldPredicate,
                               tokenFieldPredicate,
                               commonIntegerPredicate,
                               sameThreeCharStartPredicate,
                               sameFiveCharStartPredicate,
                               sameSevenCharStartPredicate,
                               nearIntegersPredicate,
                               commonFourGram,
                               commonSixGram,
                               )

        blocker = blocking.Blocking(self.training_pairs,
                                    predicate_functions,
                                    self.data_model)

        predicates = blocker.trainBlocking()

        return predicates

    def mapBlocking(self, data_d, semi_supervised=True):
        self.blocked_map = blocking.blockingIndex(data_d,
                                                  self.predicates)

    def identifyCandidates(self):
        self.candidates = blocking.mergeBlocks(self.blocked_map)

    def score(self, data_d, threshold=None):
        self.dupes = core.scoreDuplicates(self.candidates,
                                          data_d,
                                          self.data_model,
                                          threshold)

    def findDuplicates(self,
                       data_d,
                       semi_supervised=True,
                       threshold=None,
                       ):



        self.mapBlocking(data_d)
        self.identifyCandidates()
        self.printBlockingSummary(data_d)
        print 'finding duplicates ...'
        self.score(data_d, threshold)



    def printLearnedWeights(self):
        print 'Learned Weights'
        for (k1, v1) in self.data_model.items():
            try:
                for (k2, v2) in v1.items():
                    print (k2, v2['weight'])
            except:
                print (k1, v1)

    def printBlockingSummary(self, data_d):
        print 'Blocking reduced the number of comparisons by',
        print int((1 - len(self.candidates) / float(0.5 * len(data_d) ** 2)) * 100),
        print '%'
        print "We'll make",
        print len(self.candidates),
        print 'comparisons.'

    def writeSettings(self, file_name):
        source_predicates = []
        for predicate_tuple in self.predicates:
            source_predicate = []
            for predicate in predicate_tuple:
                source_predicate.append((predicate[0].__name__,
                                         predicate[1]))
            source_predicates.append(source_predicate)

        with open(file_name, 'w') as f:
            json.dump({'data model': self.data_model,
                      'predicates': source_predicates}, f)

    def readSettings(self, file_name):
        with open(file_name, 'r') as f:
            learned_settings = json.load(f)

        self.data_model = learned_settings['data model']
        self.predicates = []
        for predicate_l in learned_settings['predicates']:
            predicate_tuple = tuple([(eval(predicate[0]), predicate[1])
                                    for predicate in predicate_l])
            self.predicates.append(predicate_tuple)

    def writeTraining(self, file_name):
        with open(file_name, 'w') as f:
            json.dump(self.training_pairs, f)

    def readTraining(self, file_name, training_pairs):
        with open(file_name, 'r') as f:
            training_pairs_raw = json.load(f)

        training_pairs = {0: [], 1: []}
        for (label, examples) in training_pairs_raw.iteritems():
            for pair in examples:
                training_pairs[int(label)].append((core.frozendict(pair[0]),
                                                   core.frozendict(pair[1])))

        training_data = training_sample.addTrainingData(training_pairs,
                                                        self.data_model,
                                                        training_data)

        return training_pairs, training_data


