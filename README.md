# Dedupe Examples

Adding Athena Example scripts for the [dedupe](https://github.com/dedupeio/dedupe), a library that uses machine learning to perform de-duplication and entity resolution quickly on structured data.

Part of the [Dedupe.io](https://dedupe.io/) cloud service and open source toolset for de-duplicating and finding fuzzy matches in your data. For more details, see the [differences between Dedupe.io and the dedupe library](https://dedupe.io/documentation/should-i-use-dedupeio-or-the-dedupe-python-library.html).

To get the athena examples:
```bash
git clone https://github.com/asajadi/dedupe-examples.git
cd dedupe-examples
```

or [download this repository](https://github.com/dedupeio/dedupe-examples/archive/master.zip)

```bash
cd /path/to/downloaded/file
unzip master.zip
cd dedupe-examples
```

### Setup
We recommend using [virtualenv](http://virtualenv.readthedocs.io/en/stable/) and [virtualenvwrapper](http://virtualenvwrapper.readthedocs.org/en/latest/install.html) for working in a virtualized development environment. [Read how to set up virtualenv](http://docs.python-guide.org/en/latest/dev/virtualenvs/).

Once you have virtualenvwrapper set up,

```bash
mkvirtualenv dedupe-examples
pip install -r requirements.txt
```

Afterwards, whenever you want to work on dedupe-examples,

```bash
workon dedupe-examples
```


### [athena example](https://dedupeio.github.io/dedupe-examples/docs/mysql_example.html) - IL campaign contributions

Takes a database of IL campaign contribution data, loads it in to a
Athena database, and identifies the unique donors. 

To follow this example you need to 

* Create a Athena database called 'contributions'
* Update `athena_example/config.py` with your Athena credentials
* Install dependencies, `pip install -r requirements.txt`

Once that's all done you can run the example:

```bash
cd mysql_example
python athena_init_db.py 
python athena_example.py
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
