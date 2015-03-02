# Dedupe Examples

Example scripts for the [dedupe](https://github.com/datamade/dedupe), a library that uses machine learning to perform de-duplication and entity resolution quickly on structured data.

To get these examples:
```bash
git clone https://github.com/datamade/dedupe-examples.git
cd dedupe-examples
```

or [download this repository](https://github.com/datamade/dedupe-examples/archive/master.zip)

```bash
cd /path/to/downloaded/file
unzip master.zip
cd dedupe-examples
```

### [CSV example](http://datamade.github.com/dedupe-examples/docs/csv_example.html) - early childhood locations

This example works with a list of early childhood education sites in Chicago from 10 different sources.

```bash
cd csv_example
python csv_example.py
```
  (use 'y', 'n' and 'u' keys to flag duplicates for active learning, 'f' when you are finished)
  
**To see how you might use dedupe with smallish data, see the [annotated source code for csv_example.py](http://datamade.github.com/dedupe-examples/docs/csv_example.html).**

### [Patent example](http://datamade.github.io/dedupe-examples/docs/patent_example.html) -  patent holders

This example works with Dutch inventors from the PATSTAT international patent data file

```bash
cd patent_example
pip install unidecode
python patent_example.py
```
  (use 'y', 'n' and 'u' keys to flag duplicates for active learning, 'f' when you are finished)

### [Record Linkage example](http://datamade.github.com/dedupe-examples/docs/record_linkage_example.html) -  electronics products
This example links two spreadsheets of electronics products and links up the matching entries. Each dataset individually has no duplicates.

```bash
cd record_linkage_example
python record_linkage_example.py 
```

**To see how you might use dedupe for linking datasets, see the [annotated source code for record_linkage_example.py](http://datamade.github.com/dedupe-examples/docs/record_linkage_example.html).**

### [MySQL example](http://datamade.github.com/dedupe-examples/docs/mysql_example.html) - IL campaign contributions

Takes a database of IL campaign contribution data, loads it in to a MySQL database, and identifies the unique donors. This can take a few hours and will noticeably tax your laptop. You might want to run it overnight.

To follow this example you need to 

* Create a MySQL database called 'contributions'
* Copy `mysql_example/mysql.cnf_LOCAL` to `mysql_example/mysql.cnf`
* Update `mysql_example/mysql.cnf` with your MySQL username and password
* `easy_install MySQL-python` or `pip install MySQL-python`

Once that's all done you can run the example:

```bash
cd mysql_example
python mysql_init_db.py 
python mysql_example.py
```
  (use 'y', 'n' and 'u' keys to flag duplicates for active learning, 'f' when you are finished) 

**To see how you might use dedupe with bigish data, see the [annotated source code for mysql_example](http://datamade.github.com/dedupe-examples/docs/mysql_example.html).** 


### [PostgreSQL big dedupe example](http://datamade.github.io/dedupe-examples/docs/pgsql_big_dedupe_example.html) - PostgreSQL example on large dataset

This is the same example as the MySQL IL campaign contributions dataset above, but ported to run on PostgreSQL.

To follow this example you need to:

* Create a PostgreSQL database
* `easy_install psycopg2` or `pip install psycopg2`
* `easy_install dj-database-url` or `pip install dj-database-url`
* `easy_install unidecode` or `pip install unidecode`
* Set an environment variable with your PostgreSQL connection details: `export DATABASE_URL=postgres://user:password@host/mydatabase`

Once that's all done you can run the example:

```bash
cd pgsql_big_dedupe_example
python pgsql_big_dedupe_example_init_db.py 
python pgsql_big_dedupe_example.py
```
  (use 'y', 'n' and 'u' keys to flag duplicates for active learning, 'f' when you are finished) 


## Training

The _secret sauce_ of dedupe is human input. In order to figure out the best rules to deduplicate a set of data, you must give it a set of labeled examples to learn from. 

The more labeled examples you give it, the better the deduplication results will be. At minimum, you should try to provide __10 positive matches__ and __10 negative matches__.

The results of your training will be saved in a JSON file for future runs of dedupe.

Here's an example labeling operation:

```bash
Phone :  2850617
Address :  3801 s. wabash
Zip :
Site name :  ada s. mckinley st. thomas cdc

Phone :  2850617
Address :  3801 s wabash ave
Zip :
Site name :  ada s. mckinley community services - mckinley - st. thomas

Do these records refer to the same thing?
(y)es / (n)o / (u)nsure / (f)inished
```

