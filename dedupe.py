from itertools import combinations
from random import sample, shuffle
import affinegap
import lr
from blocking import trainBlocking, blockingIndex, mergeBlocks, allCandidates
from predicates import *
from math import log, exp


def createTrainingPairs(data_d,
                        duplicates_s,
                        n_training_dupes,
                        n_training_distinct) :
  duplicates = []

  duplicates_set = list(duplicates_s)
  shuffle(duplicates_set)


  for random_pair in duplicates_set :
    training_pair = (data_d[tuple(random_pair)[0]],
                     data_d[tuple(random_pair)[1]])      
    duplicates.append(training_pair)
    if len(duplicates) == n_training_dupes :
      break

  nonduplicates = []

  all_pairs = list(combinations(data_d, 2))

  for random_pair in sample(all_pairs,
                            n_training_dupes + n_training_distinct) :
    training_pair = (data_d[tuple(random_pair)[0]],
                     data_d[tuple(random_pair)[1]])
    if set(random_pair) not in duplicates_s :
      nonduplicates.append(training_pair)
    if len(nonduplicates) == n_training_distinct :
      break
      
  return({0:nonduplicates, 1:duplicates})

def calculateDistance(instance_1, instance_2, fields) :
  distances_d = {}
  for name in fields :
    if fields[name]['type'] == 'String' :
      distanceFunc = affinegap.normalizedAffineGapDistance
    #distances_d[name] = distanceFunc(instance_1[name],instance_2[name], -1, 1, 0.8, 0.2)
    distances_d[name] = distanceFunc(instance_1[name],instance_2[name], -5, 5, 4, 1, .125)

  return distances_d

def findUncertainPairs(data_d, data_model, num_pairs) :
  pairs = allCandidates(data_d) 

  duplicateScores = scorePairs(pairs, data_d, data_model)

  uncertain_pairs = []
  for scored_pair in duplicateScores :
    pair = scored_pair.keys()[0]
    probability = scored_pair.values()[0]

    try:
      entropy = -(probability * log(probability) +
                  (1-probability) * log(1-probability))
    except:
      print probability

    uncertain_pairs.append({pair : entropy})

    
  uncertain_pairs = sorted(uncertain_pairs, key=lambda(d): -d.values()[0])

  return uncertain_pairs[0:num_pairs]

def activeLearning(data_d, data_model, labelPairFunction) :

  training_data = []
  fic_score = 0

  while convergence == False :
    uncertain_pairs = findUncertainPairs(data_d, data_model, 1)
    labeled_pairs = labelPairFunction(uncertain_pairs)
    training_data = addTrainingData(labeled_pairs, training_data)
    data_model = train_model(training_data, iterations, data_model)
    old_fic_score = fic_score(data_model)
    fic_score = fischerInformation(data_model)
    if fic_score - old_fic_score < epsilon :
      convergence = True
  
  return(training_data, data_model)

def fischerInformation(data_model) :
  fic_score = 0
  
  return fic_score
  
def addTrainingData(labeled_pairs, training_data) :

  for label, examples in labeled_pairs.items() :
      for pair in examples :
          distances = calculateDistance(pair[0],
                                        pair[1],
                                        data_model['fields'])
          training_data.append((label, distances))
          
  return training_data


def createTrainingData(training_pairs) :
  training_data = []

  for label, examples in training_pairs.items() :
      for pair in examples :
          distances = calculateDistance(pair[0],
                                        pair[1],
                                        data_model['fields'])
          training_data.append((label, distances))

  shuffle(training_data)
  return training_data

def trainModel(training_data, iterations, data_model) :
    trainer = lr.LogisticRegression()
    trainer.train(training_data, iterations)

    data_model['bias'] = trainer.bias
    for name in data_model['fields'] :
        data_model['fields'][name]['weight'] = trainer.weight[name]

    return(data_model)



def scorePairs(candidates, data_d, data_model) :
  duplicateScores = []

  for pair in candidates :
    fields = data_model['fields']
    distances = calculateDistance(data_d[pair[0]], data_d[pair[1]], fields)
    
    score = data_model['bias'] 
    for name in fields :
      score += distances[name] * fields[name]['weight']

    score = exp(score)/(1 + exp(score))
    duplicateScores.append({ pair : score })

  return(duplicateScores)


def findDuplicates(candidates, data_d, data_model, threshold) :

  duplicateScores = scorePairs(candidates, data_d, data_model)

  return([pair for pair in duplicateScores if pair.values()[0] > threshold])


if __name__ == '__main__':
  from test_data import init

  def consoleLabel(uncertain_pairs) :
    duplicates = []
    nonduplicates = []
    
    for pair in uncertain_pairs :
      label = ''
      
      for instance in tuple(pair) :
        print instance.values()
        print "Do these records refer to the same thing?"  
      
        valid_response = False
        while not valid_response :
          label = raw_input('yes(y)/no(n)/unsure(u)\n')
          if label in ['y', 'n', 'u'] :
            valid_response = True

        if label == 'y' :
          duplicates.append(pair)
        elif label == 'n' :
          nonduplicates.append(pair)
        elif label != 'u' :
          print 'Nonvalid response'
          raise

    return({0:nonduplicates, 1:duplicates})

  
  num_training_dupes = 200
  num_training_distinct = 16000
  numIterations = 100

  import time
  t0 = time.time()
  (data_d, duplicates_s, data_model) = init()
  #candidates = allCandidates(data_d)
  #print "training data: "
  #print duplicates_s

  print "number of duplicates pairs"
  print len(duplicates_s)
  print ""

  training_pairs = createTrainingPairs(data_d,
                                       duplicates_s,
                                       num_training_dupes,
                                       num_training_distinct)

  predicates = trainBlocking(training_pairs,
                            (wholeFieldPredicate,
                             tokenFieldPredicate,
                             commonIntegerPredicate,
                             sameThreeCharStartPredicate,
                             sameFiveCharStartPredicate,
                             sameSevenCharStartPredicate,
                             nearIntegersPredicate,
                             commonFourGram,
                             commonSixGram),
                             data_model, 1, 1)


  blocked_data = blockingIndex(data_d, predicates)
  candidates = mergeBlocks(blocked_data)



  print ""
  print "Blocking reduced the number of comparisons by",
  print int((1-len(candidates)/float(0.5*len(data_d)**2))*100),
  print "%"
  print "We'll make",
  print len(candidates),
  print "comparisons."

  training_data = createTrainingData(training_pairs)
  #print "training data from known duplicates: "
  #for instance in training_data :
  #  print instance

  print ""
  print "number of training items: "
  print len(training_data)
  print ""

  print "training weights ..."
  data_model = trainModel(training_data, numIterations, data_model)
  print ""

  print "Learned Weights"
  for k1, v1 in data_model.items() :
    try:
      for k2, v2 in v1.items() :
        print (k2, v2['weight'])
    except :
      print (k1, v1)

  print ""
  
  print "finding duplicates ..."
  print ""
  dupes = findDuplicates(candidates, data_d, data_model, .60)

  dupe_ids = set([frozenset(list(dupe_pair.keys()[0])) for dupe_pair in dupes])
  true_positives = dupe_ids & duplicates_s
  false_positives = dupe_ids - duplicates_s
  uncovered_dupes = duplicates_s - dupe_ids

  print "False negatives" 
  for pair in uncovered_dupes :
         print ""
         for instance in tuple(pair) :
           print data_d[instance].values()

  print "____________________________________________"
  print "False positives" 

  for pair in false_positives :
    print ""
    for instance in tuple(pair) :
      print data_d[instance].values()

  print ""

  print "found duplicate"
  print len(dupes)

  print "precision"
  print (len(dupes) - len(false_positives))/float(len(dupes))

  print "recall"
  print  len(true_positives)/float(len(duplicates_s))
  print "ran in ", time.time() - t0, "seconds"

  print findUncertainPairs(data_d, data_model, 10)


  

