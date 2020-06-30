# MySQL Example

Takes a database of IL campaign contribution data, loads it in to a
MySQL database, and identifies the unique donors. This can take a few
hours and will noticeably tax your laptop. You might want to run it
overnight.

To follow this example you need to 

* Create a MySQL database called 'contributions'
* Copy `mysql_example/mysql.cnf_LOCAL` to `mysql_example/mysql.cnf`
* Update `mysql_example/mysql.cnf` with your MySQL username and password
* Install dependencies, `pip install -r requirements.txt`

Once that's all done you can run the example:

```bash
cd mysql_example
python mysql_init_db.py 
python mysql_example.py
```

  (use 'y', 'n' and 'u' keys to flag duplicates for active learning, 'f' when you are finished) 
