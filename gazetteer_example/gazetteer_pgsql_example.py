import itertools

import dedupe
import psycopg2
import dj_database_url

# Set up a database connection. This example uses PostgreSQL and psycopg2, but
# other databases should work well too.
db_conf = dj_database_url.config()

if not db_conf:
    raise Exception(
        'set DATABASE_URL environment variable with your connection, e.g. '
        'export DATABASE_URL=postgres://user:password@host/mydatabase'
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

        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS indexed_records
            (block_key text, record_id INT, UNIQUE(block_key, record_id)
        ''')
        cursor.executemany(
            'REPLACE INTO indexed_records VALUES (?, ?)',
            self.blocker(data.items(), target=True)
        )
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS
            indexed_records_block_key_idx
            ON indexed_records
            (block_key)
        ''')
        cursor.commit()
        cursor.close()

    def _blockData(self, messy_data):
        cursor = conn.cursor('gen_cursor')
        cursor.execute('BEGIN')
        cursor.execute('CREATE TEMPORARY TABLE blocking_map (block_key text, record_id INT)')
        cursor.executemany(
            'INSERT INTO blocking_map VALUES (?, ?)',
            self.blocker(messy_data.items(), target=True)
        )
        cursor.execute('''
            SELECT DISTINCT a.record_id, b.record_id, gaz.*
            FROM blocking_map a
            INNER JOIN indexed_records b
            USING (block_key)
            INNER JOIN gazetteer gaz
            ON b.record_id = gaz.id
            ORDER BY a.record_id
        ''')

        # Group and yield pairs.
        pair_blocks = itertools.groupby(cursor, lambda x: x[0])
        for _, pair_block in pair_blocks:
            yield [((a_record_id, messy_data[a_record_id]),
                    # TODO: Need to figure out how to format gazetteer data correctly.
                    (b_record_id, dict(gaz_data)))
                   for a_record_id, b_record_id, *gaz_data
                   in pair_block]

        cursor.execute('ROLLBACK')
        cursor.close()


if __name__ == '__main__':

    cursor = conn.cursor()

    # Load database tables
    canon_file = 'AbtBuy_Buy.csv'
    messy_file = 'AbtBuy_Abt.csv'

    print('Importing raw data from CSV...')
    cursor.execute('DROP TABLE IF EXISTS messy')
    cursor.execute('''
        CREATE TABLE messy
        (id INT, title VARCHAR(255), description VARCHAR(255), price MONEY)
    ''')
    with open(messy_file, 'rU') as fobj:
        cursor.copy_expert(
            'COPY messy (id, title, description, price) FROM STDIN CSV HEADER',
            fobj
        )
    cursor.commit()

    cursor.execute('DROP TABLE IF EXISTS canonical')
    cursor.execute('''
        CREATE TABLE canonical
        (id INT, title VARCHAR(255), description VARCHAR(255), price MONEY)
    ''')
    with open(canon_file, 'rU') as fobj:
        cursor.copy_expert(
            'COPY canonical (id, title, description, price) FROM STDIN CSV HEADER',
            fobj
        )
    cursor.commit()

    # Retrieve incoming messy records from the database.
    cursor.execute('''
        SELECT *
        FROM messy
    ''')
    messy_records = {row['id']: dict(row) for row in cursor.fetchall()}

    # Retrieve all canonical records from the database.
    cursor.execute('''
        SELECT *
        FROM canonical
    ''')
    canonical_records = {row['id']: dict(row) for row in cursor.fetchall()}

    # Rehydrate Deduper from an existing learned settings file.
    # Instead of loading the settings file from the filesystem, you could also
    # imagine saving it to the database as a pickled object and then retrieving it
    # with a database query.
    with open('gazetteer_learned_settings', 'rb') as settings_file:
        gazetteer = StaticDatabaseGazetteer(settings_file)

    # Index the canonical records.
    gazetteer.index(canonical_records)

    # Calculate the threshold.
    threshold = gazetteer.threshold(messy_records, recall_weight=1.0)

    # Match the incoming records at the calculated threshold, returning one match
    # per row in generator form. If any record doesn't have a match over the
    # threshold, it won't be returned in `results`.
    results = gazetteer.match(messy_records, threshold=threshold)

    # Update the messy data to assign the new matches.
    for matches in results:
        for (messy_id, canonical_id), score in matches:
            cursor.execute('''
                UPDATE messy
                SET canonical_id = %s
                WHERE id = %s
            ''', (canonical_id, messy_id)
            )

    # Update the canonical dataset to insert any records that didn't have a
    # satisfactory match.
    cursor.execute('''
        INSERT INTO canonical
        SELECT id
        FROM messy
        WHERE messy.canonical_id IS NULL
    ''')

    # Commit and close the database connection.
    conn.commit()
    conn.close()
