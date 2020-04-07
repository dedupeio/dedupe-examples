import itertools
import os

import dedupe
import psycopg2
import psycopg2.extras
import dj_database_url

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
    Subclass StaticGazetteer to interact with the database in indexing and matching.
    """
    def index(self, data):
        """
        Add records to the index of records to match against.

        Override this method to interact with a Postgres database instead of
        producing a local index.
        """
        # Get blocks for the records.
        # (In Dedupe 2.x, replace this with self.fingerprinter)
        self.blocker.indexAll(data)

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

            # Group and yield pairs.
            pair_blocks = itertools.groupby(cursor, lambda row: row['blocking_record_id'])
            for _, pair_block in pair_blocks:
                messy_record = []
                blocked_records = []
                for row in pair_block:
                    if len(messy_record) == 0:
                        # Append the unmatched record.
                        blocking_record_id = row['blocking_record_id']
                        messy_record = [(blocking_record_id, messy_data[blocking_record_id], set())]

                    index_record_id = row['index_record_id']
                    index_record = {k: v for k, v in row.items() if not k.endswith('_record_id')}
                    blocked_records.append(
                        (index_record_id, index_record, set())
                    )
                yield [messy_record, blocked_records]

            cursor.execute('DROP TABLE blocking_map')


if __name__ == '__main__':

    # Load database tables
    canon_file = 'AbtBuy_Buy.csv'
    messy_file = 'AbtBuy_Abt.csv'

    print('Importing raw data from CSV...')
    with conn.cursor() as cursor:
        cursor.execute('DROP TABLE IF EXISTS messy')
        cursor.execute('''
            CREATE TABLE messy
            (id INT, title TEXT, description TEXT, price MONEY, canonical_id INT)
        ''')
        with open(messy_file) as fobj:
            cursor.copy_expert(
                'COPY messy (id, title, description, price) FROM STDIN CSV HEADER',
                fobj
            )
        cursor.execute('''
            ALTER TABLE messy ALTER COLUMN price TYPE float USING price::numeric::float
        ''')

        cursor.execute('DROP TABLE IF EXISTS gazetteer')
        cursor.execute('''
            CREATE TABLE gazetteer
            (id INT, title TEXT, description TEXT, price MONEY)
        ''')
        with open(canon_file) as fobj:
            cursor.copy_expert(
                'COPY gazetteer (id, title, description, price) FROM STDIN CSV HEADER',
                fobj
            )
        cursor.execute('''
            ALTER TABLE gazetteer ALTER COLUMN price TYPE float USING price::numeric::float
        ''')

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

    # Rehydrate Deduper from an existing learned settings file.
    # Instead of loading the settings file from the filesystem, you could also
    # imagine saving it to the database as a pickled object and then retrieving it
    # with a database query.
    if not os.path.exists('gazetteer_learned_settings'):
        raise FileNotFoundError(
            'gazetteer_learned_settings file not found. '
            'Did you run gazetteer_example.py before running this script?'
        )

    print('\nLoading Gazetteer from settings file...')
    with open('gazetteer_learned_settings', 'rb') as settings_file:
        gazetteer = StaticDatabaseGazetteer(settings_file)

    # Index the canonical records.
    print('\nIndexing canonical records...')
    gazetteer.index(canonical_records)

    # Calculate the threshold.
    print('\nCalculating threshold...')
    threshold = gazetteer.threshold(messy_records, recall_weight=1.0)

    # Match the incoming records at the calculated threshold, returning one match
    # per row in generator form. If any record doesn't have a match over the
    # threshold, it won't be returned in `results`.
    print('\nMatching messy records...')
    results = gazetteer.match(messy_records, threshold=threshold)

    # Update the messy data to assign the new matches.
    print('\nUpdating messy data with match IDs...')
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
    print('\nUpdating canonical data with unmatched records...')
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
