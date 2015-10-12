# Big PostgreSQL example

To follow this example you need to:

* Install dependencies `pip install -r requirements.txt`
* Create a PostgreSQL database
* Set an environment variable with your PostgreSQL connection details: `export DATABASE_URL=postgres://user:password@host/mydatabase`

Once that's all done you can run the example:

```bash
python pgsql_big_dedupe_example_init_db.py 
python pgsql_big_dedupe_example.py
```
