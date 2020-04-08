#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
This code demonstrates Gazetteer matching backed by a Postgres database.

NOTE: This script uses the dedupe 1.x API. For an example of dedupe 2.x, see
gazetteer_example.py.
"""

import itertools
import os
import io
import csv

import dedupe
import psycopg2
import psycopg2.extras
import dj_database_url

from gazetteer_example import readData, preProcess

# Set up a database connection. This example uses PostgreSQL and psycopg2, but
# other databases should work well too.
db_conf = dj_database_url.config()

if not db_conf:
    raise Exception(
        'set DATABASE_URL environment variable with your connection, e.g. '
        'export DATABASE_URL=postgres://user:password@host:port/mydatabase'
    )

conn = psycopg2.connect(
    database=db_conf['NAME'],
    user=db_conf['USER'],
    password=db_conf['PASSWORD'],
    host=db_conf['HOST'],
    cursor_factory=psycopg2.extras.RealDictCursor
)


class StaticDatabaseGazetteer(dedupe.StaticGazetteer):
    """
    Subclass StaticGazetteer to interact with the database in indexing and matching
    by storing blocks in the database instead of in memory.
    """
    def index(self, data):
        """
        Add records to the index of records to match against.

        Override this method to interact with a Postgres database instead of
        producing a local index.
        """
        # Get blocks for the records.
        self.blocker.indexAll(data)

        # Save blocks to the database.
        with conn.cursor() as cursor:
            cursor.execute('DROP TABLE IF EXISTS indexed_records')
            cursor.execute('''
                CREATE TABLE indexed_records
                (block_key text, record_id INT, UNIQUE(block_key, record_id))
            ''')
            cursor.executemany(
                'INSERT INTO indexed_records VALUES (%s, %s)',
                self.blocker(data.items(), target=True)
            )
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS
                indexed_records_block_key_idx
                ON indexed_records
                (block_key)
            ''')

    def _blockData(self, messy_data):
        """
        Retrieve blocks for messy_data by querying the database.

        This method is used by the match() and threshold() methods for blocking,
        so by overriding it we can force those methods to read blocks
        from the database instead of from memory.
        """
        with conn.cursor() as cursor:
            cursor.execute('CREATE TEMPORARY TABLE blocking_map (block_key text, record_id INT)')
            cursor.executemany(
                'INSERT INTO blocking_map VALUES (%s, %s)',
                self.blocker(messy_data.items(), target=True)
            )
            cursor.execute('''
                SELECT DISTINCT
                    a.record_id AS blocking_record_id,
                    b.record_id AS index_record_id,
                    gaz.*
                FROM blocking_map a
                INNER JOIN indexed_records b
                USING (block_key)
                INNER JOIN gazetteer gaz
                ON b.record_id = gaz.id
                ORDER BY a.record_id
            ''')

            # Group and yield pair blocks. Pair blocks are formatted as a list of
            # two sequences A and B, where A represents the record and B represents
            # all of the records it is blocked against. To produce this sequence,
            # we need to group our query by the blocking record ID (A) and create
            # a sequence of all of the index records it is blocked against (B).
            pair_blocks = itertools.groupby(cursor, lambda row: row['blocking_record_id'])
            for _, pair_block in pair_blocks:
                messy_record = []
                blocked_records = []
                for row in pair_block:
                    if len(messy_record) == 0:
                        # The blocking record has not yet been set yet, so
                        # set it before beginning to aggregate the index records.
                        blocking_record_id = row['blocking_record_id']
                        # The proper format for a sequence in a pair block is
                        # (record_id, record_dict, smaller_blocks). For initialization
                        # purposes, we can set the smaller_blocks to an empty set.
                        messy_record = [(blocking_record_id, messy_data[blocking_record_id], set())]

                    index_record_id = row['index_record_id']
                    index_record = {k: v for k, v in row.items() if not k.endswith('_record_id')}
                    blocked_records.append(
                        (index_record_id, index_record, set())
                    )

                # Yield from the query to prevent loading all blocks into memory.
                # In non-Postgres databases, check your configuration to confirm
                # that your cursor supports generators.
                yield [messy_record, blocked_records]

            # Clean up the temporary blocking map table after the query has
            # been exhausted.
            cursor.execute('DROP TABLE blocking_map')


def read_data_for_postgres(filename):
    """
    Helper function to read in data from a CSV and prep it for importing to
    Postgres by cleaning fields and returning a file-like CSV object.
    """
    output_fobj = io.StringIO()

    with open(filename) as fobj:
        reader = csv.DictReader(fobj)
        fieldnames = ['id', 'title', 'description', 'price']
        writer = csv.DictWriter(output_fobj, fieldnames=fieldnames)
        writer.writeheader()

        for idx, row in enumerate(reader):
            clean_row = dict([(k, preProcess(v)) for k, v in row.items()])
            if clean_row['price']:
                clean_row['price'] = float(clean_row['price'][1:])
            if clean_row['unique_id']:
                del clean_row['unique_id']
            clean_row['id'] = idx
            writer.writerow(clean_row)

    output_fobj.seek(0)
    return output_fobj


def descriptions(datasets):
    """Helper function for yielding description corpuses from datasets."""
    for dataset in datasets:
        for record in dataset.values():
            yield record['description']


if __name__ == '__main__':

    # Load database tables.
    canon_file = os.path.join('data', 'AbtBuy_Buy.csv')
    messy_file = os.path.join('data', 'AbtBuy_Abt.csv')

    print('Importing raw data into the database')
    canonical = readData(canon_file)
    messy = readData(messy_file)

    with conn.cursor() as cursor:
        cursor.execute('DROP TABLE IF EXISTS messy')
        cursor.execute('''
            CREATE TABLE messy
            (id INT, title TEXT, description TEXT, price FLOAT, canonical_id INT)
        ''')
        cursor.copy_expert(
            'COPY messy (id, title, description, price) FROM STDIN WITH CSV HEADER',
            read_data_for_postgres(messy_file),
        )

        cursor.execute('DROP TABLE IF EXISTS gazetteer')
        cursor.execute('''
            CREATE TABLE gazetteer
            (id INT, title TEXT, description TEXT, price FLOAT)
        ''')
        cursor.copy_expert(
            'COPY gazetteer (id, title, description, price) FROM STDIN WITH CSV HEADER',
            read_data_for_postgres(canon_file),
        )

    # Rehydrate Deduper from an existing learned settings file.
    # Instead of loading the settings file from the filesystem, you could also
    # imagine saving it to the database as a pickled object and then retrieving it
    # with a database query.
    settings_file = 'gazetteer_learned_settings'
    training_file = 'gazetteer_training.json'

    if os.path.exists(settings_file):
        print('\nLoading Gazetteer from settings file %s' % settings_file)
        with open(settings_file, 'rb') as settings_fobj:
            gazetteer = StaticDatabaseGazetteer(settings_fobj)
    else:
        # Define the fields the Gazetteer will pay attention to.
        print('\nSettings file %s does not exist, initializing training' % settings_file)
        fields = [
            {'field': 'title', 'type': 'String'},
            {'field': 'title', 'type': 'Text', 'corpus': descriptions([messy, canonical])},
            {'field': 'description', 'type': 'Text',
             'has missing': True, 'corpus': descriptions([messy, canonical])},
            {'field': 'price', 'type': 'Price', 'has missing': True}]

        # Create a new gazetteer object and pass our data model to it.
        gazetteer = dedupe.Gazetteer(fields)

        # To train the gazetteer, we feed it a sample of records.
        gazetteer.sample(messy, canonical, 15000)

        # If we have training data saved from a previous run,
        # look for it an load it in.
        if os.path.exists(training_file):
            print('\nReading labeled examples from %s' % training_file)
            with open(training_file) as tf:
                gazetteer.readTraining(tf)

        # Initialize active learning.
        dedupe.consoleLabel(gazetteer)

        gazetteer.train()

        # When finished, save training and settings to disk.
        print('\nSaving training data to %s' % training_file)
        with open(training_file, 'w') as tf:
            gazetteer.writeTraining(tf)

        print('\nSaving settings file to %s' % settings_file)
        with open(settings_file, 'wb') as sf:
            gazetteer.writeSettings(sf)

        # Free up memory from training.
        gazetteer.cleanupTraining()

    # Retrieve incoming messy records from the database.
    with conn.cursor() as cursor:
        cursor.execute('''
            SELECT *
            FROM messy
        ''')
        messy_records = {row['id']: dict(row) for row in cursor.fetchall()}

        # Retrieve all canonical records from the database.
        cursor.execute('''
            SELECT *
            FROM gazetteer
        ''')
        canonical_records = {row['id']: dict(row) for row in cursor.fetchall()}

    # Index the canonical records.
    print('\nIndexing canonical records')
    gazetteer.index(canonical_records)

    # Calculate the threshold.
    print('\nCalculating threshold')
    threshold = gazetteer.threshold(messy_records, recall_weight=1.0)

    # Match the incoming records at the calculated threshold, returning one match
    # per row in generator form. If any record doesn't have a match over the
    # threshold, it won't be returned in `results`.
    print('\nMatching messy records')
    results = gazetteer.match(messy_records, threshold=threshold)

    # Update the messy data to assign the new matches.
    print('\nUpdating messy data with match IDs')
    with conn.cursor() as cursor:
        counter = 0
        for matches in results:
            for (messy_id, canonical_id), score in matches:
                cursor.execute('''
                    UPDATE messy
                    SET canonical_id = %s
                    WHERE id = %s
                ''', (int(canonical_id), int(messy_id))
                )
                counter += 1
    print('Updated %d matches' % counter)

    # Update the canonical dataset to insert any records that didn't have a
    # satisfactory match.
    print('\nUpdating canonical data with unmatched records')
    with conn.cursor() as cursor:
        cursor.execute('''
            WITH unmatched_rows AS (
                INSERT INTO gazetteer
                SELECT id
                FROM messy
                WHERE messy.canonical_id IS NULL
                RETURNING 1
            )
            SELECT count(*) from unmatched_rows
        ''')
        count = cursor.fetchone()['count']
        print('Updated %d unmatched rows' % count)

    # Commit and close the database connection.
    conn.commit()
    conn.close()
