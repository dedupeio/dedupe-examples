"""
This is an example of working with very large data. There are about
700,000 unduplicated donors in this database of Illinois political
campaign contributions.

With such a large set of input data, we cannot store all the comparisons
we need to make in memory. Instead, we will read the pairs on demand
from the Mongo database.

__Note:__ You will need to run `python patient.py`

For smaller datasets (<10,000), see our
[csv_example](csv_example.html)
"""
from __future__ import print_function

import os
import itertools
import time
import logging
import optparse
import locale
import pickle
import multiprocessing
import pymongo
import codecs
import dedupe
import sys

from unidecode import unidecode



# ## Logging

# Dedupe uses Python logging to show or suppress verbose output. Added
# for convenience.  To enable verbose output, run `python
# examples/mysql_example/mysql_example.py -v`

reload(sys)
sys.setdefaultencoding('utf8')


optp = optparse.OptionParser()
optp.add_option('-v', '--verbose', dest='verbose', action='count',
                help='Increase verbosity (specify multiple times for more)'
                )
(opts, args) = optp.parse_args()
log_level = logging.WARNING
if opts.verbose:
    if opts.verbose == 1:
        log_level = logging.INFO
    elif opts.verbose >= 2:
        log_level = logging.DEBUG
logging.getLogger().setLevel(log_level)

# ## Setup

settings_file = 'mongo_example_settings'
training_file = 'mongo_example_training.json'

start_time = time.time()

# You'll need to copy `examples/mssql_example/mssql.cnf_LOCAL` to
# `examples/mssql_example/mssql.cnf` and fill in your mysql database
# information in `examples/mssql_example/mssql.cnf`


client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client.migracion
collection = db.pacienteHeller
#collection = db.pacienteSips


if os.path.exists(settings_file):
    print('reading from ', settings_file)
    with open(settings_file, 'rb') as sf:
        deduper = dedupe.StaticDedupe(sf, num_cores=4)
else:

    # Define the fields dedupe will pay attention to
    #
    # The address, city, and zip fields are often missing, so we'll
    # tell dedupe that, and we'll learn a model that take that into
    # account
    fields = [
        {'field': 'documento', 'type': 'String'},
        {'field': 'nombre', 'type': 'String'},
        {'field': 'apellido', 'type': 'String'},
        {'field': 'sexo', 'type': 'Exact'},
        {'field': 'fechaNacimiento', 'type': 'Exact'},
        # {'field' : 'Zip', 'type': 'Exact', 'has missing' : True},
        #{'field' : 'Phone', 'type': 'String', 'has missing' : True},
    ]
    # Create a new deduper object and pass our data model to it.
    deduper = dedupe.Dedupe(fields, num_cores=4)

    # We will sample pairs from the entire donor table for training
    # "_id": 0,
    #Se cambia idPaciente por idPacienteHeller
    temp_d = {}
    for document in list(collection.find(projection={"idPacienteHeller":1,"documento": 1,"nombre": 1,"apellido": 1,"sexo": 1,"fechaNacimiento": 1})):
        document['fechaNacimiento'] = str(document['fechaNacimiento'])
        clean_row = [(k, v) for (k, v) in document.items()]
        row_id = document['idPacienteHeller']
        #temp_d[row_id] = dict(zip(fields, [list(set(document[field])) for field in fields]))
        temp_d[row_id] = dict(clean_row)

    # temp_d = dict((i, row) for i, row in enumerate(data))

    deduper.sample(temp_d, 10000)

    del temp_d

    # If we have training data saved from a previous run of dedupe,
    # look for it an load it in.
    #
    # __Note:__ if you want to train from
    # scratch, delete the training_file

    if os.path.exists(training_file):
        print('reading labeled examples from ', training_file)
        with open(training_file) as tf:
            deduper.readTraining(tf)

    # ## Active learning

    print('starting active labeling...')
    # Starts the training loop. Dedupe will find the next pair of records
    # it is least certain about and ask you to label them as duplicates
    # or not.

    # use 'y', 'n' and 'u' keys to flag duplicates
    # press 'f' when you are finished
    dedupe.convenience.consoleLabel(deduper)

    deduper.train(maximum_comparisons=500000000, recall=0.90)
    # When finished, save our labeled, training pairs to disk
    with open(training_file, 'w') as tf:
        deduper.writeTraining(tf)

    # Notice our two arguments here
    #
    # `maximum_comparisons` limits the total number of comparisons that
    # a blocking rule can produce.
    #
    # `recall` is the proportion of true dupes pairs that the learned
    # rules must cover. You may want to reduce this if your are making
    # too many blocks and too many comparisons.

    with open(settings_file, 'wb') as sf:
        deduper.writeSettings(sf)

    # We can now remove some of the memory hobbing objects we used
    # for training
    deduper.cleanupTraining()


data_d = {}
#idPaciente por idPacienteHeller
for row in collection.find(projection={"idPacienteHeller":1,"documento": 1,"nombre": 1,"apellido": 1,"sexo": 1,"fechaNacimiento": 1}):
    clean_row = [(k,v) for (k, v) in row.items()]
    row_id = int(row['idPacienteHeller'])
    data_d[row_id] = dict(clean_row)

threshold = deduper.threshold(data_d, recall_weight=0.3)

# ## Clustering

# `match` will return sets of record IDs that dedupe
# believes are all referring to the same entity.

print('clustering...')
clustered_dupes = deduper.match(data_d, threshold)

print('# duplicate sets', len(clustered_dupes))

#Si existe la colleccion clusterPacientes se elimina

colcluster = db.clusterPacientesHeller
colcluster.delete_many({})


print('creating clusterPacientes')


listaCluster=[]

cluster_membership = {}
cluster_id = 0
for (cluster_id, cluster) in enumerate(clustered_dupes):
    id_set, scores = cluster
    cluster_d = [data_d[c] for c in id_set]
    for cd in cluster_d:
        print('Paciente',cd['_id'])
        collection.update_one({"_id":cd['_id']}, { "$addToSet": { "claveBlocking": str(cluster_id) },"$set": {"clusterId":cluster_id}} )
    print("idCluster ",cluster_id, "scores ",[str(n) for n in list(scores)],"clusterd", cluster_d)
    r ={"idCluster": cluster_id, "scores":[float(n) for n in list(scores)],"cluster": cluster_d }
    #colcluster.insert_one({"idCluster": cluster_id, "scores":list(scores),"cluster": cluster_d })
    #colcluster.insert_one(r)



# Close our database connection
client.close()
