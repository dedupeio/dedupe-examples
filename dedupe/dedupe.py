#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
import random
import collections
import itertools

import core
import training_sample
import crossvalidation
import predicates
import blocking
import clustering
import tfidf
import numpy
import dedupe

import types

class Dedupe:
    """
    Public methods:
    __init__
    initializeTraining
    train
    blockingFunction
    duplicateClusters
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
            self._readSettings(init)            
        elif init:
            raise ValueError("Incorrect Input Type: must supply either "
                             "a field definition or a settings file."
                             )
        else:
            raise ValueError("No Input: must supply either "
                             "a field definition or a settings file."
                             )
            
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
            elif v['type'] == 'Interaction' and ('interaction-terms'
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
        Loads labeled examples from file, if passed.

        Keyword arguments:
        training_file -- path to a json file of labeled examples

        """
        n_fields = len(self.data_model['fields'])

        training_dtype = [('label', 'i4'),
                          ('field_distances', 'f4', n_fields)]

        self.training_data = numpy.zeros(0, dtype=training_dtype)
        self.training_pairs = None

        if training_file :
            (self.training_pairs,
             self.training_data) = self._readTraining(training_file,
                                                     self.training_data)                    
                
    def train(self, data_d, training_source=None, key_groups=[]) :
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

        data_d = core.sampleDict(data_d, 700) #we should consider changing this
        print "data_d length: ", len(data_d)

        self.data_d = dict([(key, core.frozendict(value)) for key, value in data_d.iteritems()])

        if training_source.__class__ is str:
            print 'reading training from file'
            if not hasattr(self, 'training_data'):
                self.initializeTraining(training_source)
            
            self.training_pairs, self.training_data = self._readTraining(training_source,
                                                                        self.training_data)

        elif isinstance(training_source, types.FunctionType) :
            if not hasattr(self, 'training_data'):
                self.initializeTraining()
            
            (self.training_data,
            self.training_pairs,
            self.data_model) = training_sample.activeLearning(self.data_d,
                                                              self.data_model,
                                                              training_source,
                                                              self.training_data,
                                                              self.training_pairs,
                                                              key_groups)

        self.alpha = crossvalidation.gridSearch(self.training_data,
                                                core.trainModel,
                                                self.data_model,
                                                k=20)

        self.data_model = core.trainModel(self.training_data,
                                          self.data_model,
                                          self.alpha)

        self._printLearnedWeights()

    def blockingFunction(self, eta=1, epsilon=1):
        """
        Returns a function that takes in a record dictionary and
        returns a list of blocking keys for the record. We will
        learn the best blocking predicates if we don't have them already.

        We'll allow for predicates to be passed
        """
        if not self.predicates:
            self.predicates = self._learnBlocking(self.data_d, eta, epsilon)

        bF = blocking.Blocker(self.predicates, self.df_index)

        return bF

    #@profile
    def duplicateClusters(self,
                          blocks,
                          pairwise_threshold = .5,
                          cluster_threshold = .5):
        """
        Partitions blocked data and returns a list of clusters, where
        each cluster is a tuple of record ids

        Keyword arguments:
        blocked_data --       Dictionary where the keys are blocking predicates 
                              and the values are tuples of records covered by that 
                              predicate.
        pairwise_threshold -- Number between 0 and 1 (default is .5). We will only 
                              consider as duplicates  ecord pairs as duplicates if 
                              their estimated duplicate likelihood is greater than 
                              the pairwise threshold. 
        cluster_threshold --  Number between 0 and 1 (default is .5). Lowering the 
                              number will increase precision, raising it will increase
                              recall

        """

        candidates = (pair for block in blocks
                      for pair in itertools.combinations(block, 2))
        
        self.dupes = core.scoreDuplicates(candidates, 
                                          self.data_model,
                                          pairwise_threshold)

        clusters = clustering.hierarchical.cluster(self.dupes, cluster_threshold)

        return clusters

    def _learnBlocking(self, data_d, eta, epsilon):
        confident_nonduplicates = blocking.semiSupervisedNonDuplicates(self.data_d,
                                                                       self.data_model)
                                                                       

        self.training_pairs[0].extend(confident_nonduplicates)

        predicate_functions = (predicates.wholeFieldPredicate,
                               predicates.tokenFieldPredicate,
                               predicates.commonIntegerPredicate,
                               predicates.sameThreeCharStartPredicate,
                               predicates.sameFiveCharStartPredicate,
                               predicates.sameSevenCharStartPredicate,
                               predicates.nearIntegersPredicate,
                               predicates.commonFourGram,
                               predicates.commonSixGram,
                               )

        tfidf_thresholds = [0.2, 0.4, 0.6, 0.8]
        full_string_records = {}
        for k, v in data_d.iteritems() :
          document = ''
          for field in self.data_model['fields'].keys() :
            document += v[field]
            document += ' '
          full_string_records[k] = document

        self.df_index = tfidf.documentFrequency(full_string_records)

        blocker = blocking.Blocking(self.training_pairs,
                                    predicate_functions,
                                    self.data_model,
                                    tfidf_thresholds,
                                    self.df_index,
                                    eta,
                                    epsilon
                                    )

        learned_predicates = blocker.trainBlocking()

        return learned_predicates

    def _printLearnedWeights(self):
        print 'Learned Weights'
        for (k1, v1) in self.data_model.items():
            try:
                for (k2, v2) in v1.items():
                    print (k2, v2['weight'])
            except:
                print (k1, v1)

    def writeSettings(self, file_name):
        """
        Write to a json settings file that contains the 
        data model and predicates

        Keyword arguments:
        file_name -- path to a json file
        """

        if not file_name.endswith('.json') :
          raise ValueError("Settings file name must end with '.json'")

        source_predicates = []
        for predicate_tuple in self.predicates:
            source_predicate = []
            for predicate in predicate_tuple:
              if isinstance(predicate[0], types.FunctionType):
                source_predicate.append((predicate[0].__module__ + '.' + predicate[0].__name__,
                                         predicate[1],
                                         'simple'))
              elif predicate[0].__class__ is tfidf.TfidfPredicate :
                source_predicate.append((predicate[0].threshold,
                                         predicate[1],
                                         'tfidf'))
              else:
                raise ValueError("Undefined predicate type")
                
            source_predicates.append(source_predicate)

        with open(file_name, 'w') as f:
            json.dump({'data model': self.data_model,
                      'predicates': source_predicates}, f)

        # save df_index to its own file
        df_index_file_name = file_name.replace('.json', '') + '_df_index' + '.json'

        #print 'unseen token value:', self.df_index['UNSEEN TOKEN']
        self.df_index['UNSEEN TOKEN'] = self.df_index['UNSEEN TOKEN']
        with open(df_index_file_name, 'w') as f:
            json.dump(self.df_index, f)

    def writeTraining(self, file_name):
        """
        Write to a json file that contains labeled examples

        Keyword arguments:
        file_name -- path to a json file
        """

        with open(file_name, 'w') as f:
            json.dump(self.training_pairs, f)

    def _readSettings(self, file_name):
        with open(file_name, 'r') as f:
            learned_settings = json.loads(f.read(), object_hook=self._decode_dict)

        self.data_model = learned_settings['data model']
        self.predicates = []
        for predicate_l in learned_settings['predicates']:
          marshalled_predicate = []
          for predicate in predicate_l :
              if predicate[2] == 'simple' :
                marshalled_predicate.append((eval(predicate[0]), predicate[1]))
              elif predicate[2] == 'tfidf' : 
                marshalled_predicate.append((tfidf.TfidfPredicate(predicate[0]),
                                            predicate[1]))
  
          self.predicates.append(tuple(marshalled_predicate))

        df_index_file_name = file_name.replace('.json', '') + '_df_index' + '.json'

        with open(df_index_file_name, 'r') as f:
            df_index = json.load(f)
            unseen_value = df_index["UNSEEN TOKEN"]
            self.df_index = collections.defaultdict(lambda : unseen_value)    
            self.df_index.update(df_index)




    def _readTraining(self, file_name, training_pairs):
        with open(file_name, 'r') as f:
            training_pairs_raw = json.load(f)

        training_pairs = {0: [], 1: []}
        for (label, examples) in training_pairs_raw.iteritems():
            for pair in examples:
                training_pairs[int(label)].append((core.frozendict(pair[0]),
                                                   core.frozendict(pair[1])))

        training_data = training_sample.addTrainingData(training_pairs,
                                                        self.data_model,
                                                        self.training_data)

        return training_pairs, training_data

    # json encoding fix for unicode => string
    def _decode_list(self, data):
        rv = []
        for item in data:
            if isinstance(item, unicode):
                item = item.encode('utf-8')
            elif isinstance(item, list):
                item = self._decode_list(item)
            elif isinstance(item, dict):
                item = self._decode_dict(item)
            rv.append(item)
        return rv

    def _decode_dict(self, data):
        rv = {}
        for key, value in data.iteritems():
            if isinstance(key, unicode):
               key = key.encode('utf-8')
            if isinstance(value, unicode):
               value = value.encode('utf-8')
            elif isinstance(value, list):
               value = self._decode_list(value)
            elif isinstance(value, dict):
               value = self._decode_dict(value)
            rv[key] = value
        return rv
