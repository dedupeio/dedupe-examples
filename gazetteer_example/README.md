# Gazetteer example

This directory provides two different examples of Gazetteer matching:

- `gazetteer_example.py`: An example of how to use the Gazetteer classes for matching.
  Matching is done in memory. *This example uses the dedupe 2.x API.*
- `gazetteer_postgres_example.py`: An example of Gazetteer matching backed by a Postgres database.
  *This example uses the dedupe 1.x API.*

## Installation

All examples require an installation of [Docker](https://docs.docker.com/get-docker/)
and [Docker Compose](https://docs.docker.com/compose/install/).

Build container images to install dependencies:

```
docker-compose build
```

## Running examples

To run the in-memory Gazetteer example:

```
docker-compose run --rm gazetteer-example
```

This script will output a few artifacts, including `gazetteer_output.csv`, the
output of the matching job. To run the evaluation script:

```
docker-compose run --rm gazetteer-example python gazetteer_evaluation.py
```

Running the Gazetteer example will also produce training and settings files for
the matching model, including the files `gazetteer_training.json` and
`gazetteer_learned_settings`. Once you have these artifacts, you can also run the Postgres
example. Start by bringin up the `postgres` service in the background:

```
docker-compose up -d postgres
```

Wait a few seconds for Postgres to initialize (you can confirm this with `docker-compose logs postgres`)
and then run the example script:

```
docker-compose run --rm gazetteer-postgres-example
```

This script will output some information to the console, and update tables in the
containerized Postgres database. To inspect the matching job in those tables,
connect to the database and run `psql` commands:

```
docker-compose exec postgres psql -U postgres gazetteer_example
```


