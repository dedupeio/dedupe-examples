# Athena Example

Takes a database of IL campaign contribution data, loads it in to a
Athena database, and identifies the unique donors. 

To follow this example you need to 

* Create a Athena database called 'contributions'
* Update `athena_example/config.py` with your Athena credentials
* Install dependencies, `pip install -r requirements.txt`

Once that's all done you can run the example:

```bash
cd mysql_example
python athena_init.py 
python athena_example.py
```

  (use 'y', 'n' and 'u' keys to flag duplicates for active learning, 'f' when you are finished) 
