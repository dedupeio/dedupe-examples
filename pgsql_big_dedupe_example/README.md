# Big PostgreSQL example

To follow this example you need to:

* Install dependencies `pip install -r requirements.txt`
* Create a PostgreSQL database and setup the intarray extension
* Set an environment variable with your PostgreSQL connection details

This might look like

```bash
createdb campfin
psql -d campfin -c "CREATE EXTENSION intarray"
export DATABASE_URL=postgres:///campfin
```

Once that's all done you can run the example:

```bash
python pgsql_big_dedupe_example_init_db.py 
python pgsql_big_dedupe_example.py
```
