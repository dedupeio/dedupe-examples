#!/usr/bin/python
# -*- coding: utf-8 -*-
__all__ = ['affinegap',
           'blocking',
           'clustering',
           'core',
           'lr',
           'predicates',
           'training_sample',
           'crossvalidation',
           'dedupe',
           'distance'
           ]

#from distance import affinegap
from distance import affinegap
import distance 
import mekano
import blocking
import clustering
import core
import lr
import predicates
import training
import crossvalidation
import datamodel
import backport
from api import StaticDedupe, Dedupe
from api import StaticRecordLink, RecordLink
from core import randomPairs
from convenience import dataSample, dataSampleRecordLink
from convenience import blockData, blockDataRecordLink
from convenience import consoleLabel
import backport
