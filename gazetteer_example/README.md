# Gazetteer example

This directory provides two different examples of Gazetteer matching:

- `gazetteer_example.py`: An example of how to use the Gazetteer classes for matching.
  Matching is done in memory. *This example uses the dedupe 2.x API.*
- `gazetteer_postgres_example.py`: An example of Gazetteer matching backed by a Postgres database.
  *This example uses the dedupe 1.x API.*

## Installation

All examples require an installation of Python. We recommend you manage
Python dependencies in a virtual environment with
[virtualenvwrapper](https://virtualenvwrapper.readthedocs.io/en/latest/).

In addition, the Postgres example requires an installation of
[PostgreSQL](https://www.postgresql.org/download/).

Install dependencies for the in-memory Gazetteer example:

```
pip install -r requirements-2.x.txt
```

In a separate environment, install dependencies for the Postgres example:

```
pip install -r requirements-1.x.txt
```

Before running the Postgres example, create a database and export a
database connection string:

```bash
createdb dedupe_example
export DATABASE_URL=postgres:///dedupe_example
```

## Running examples

### In-memory Gazetteer example

To run the in-memory Gazetteer example:

```
python gazetteer_example.py
```

This script will output a few artifacts, including `gazetteer_output.csv`, the
output of the matching job. To run the evaluation script:

```
python gazetteer_evaluation.py
```

### Postgres Gazetteer example

To run the Postgres Gazetteer example:

```
python gazetteer_postgres_example.py
```

This script will output some information to the console, and update tables in
the database specified in the `DATABASE_URL` connection string. The relevant
tables will be called `messy` and `gazetteer`. Use `psql` or your favorite
Postgres client to inspect these tables once the script has completed.
