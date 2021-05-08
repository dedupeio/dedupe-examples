# Athena Example

Takes a database of IL campaign contribution data, loads it in to a
Athena database, and identifies the unique donors. 

To follow this example you need to 

* Update `athena_example/config.py` with your Athena credentials, database name and the path to sroe the data
* Install dependencies, `pip install -r requirements.txt`

Once that's all done you can run the example:

```bash
cd athena_example
python athena_init.py 
python athena_example.py
```

  (use 'y', 'n' and 'u' keys to flag duplicates for active learning, 'f' when you are finished) 
