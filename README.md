# litmus

A system for distinguishing candidate SQL queries using system-suggested tuples.

## System Setup

Create a virtual environment and activate it:

```
cd python
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
```

Also:
```
cd python
cp config.ini.example config.ini
```
Then make sure the variables set in `config.ini` are what you want (in particular, the MySQL credentials must be set correctly!).

## Datasets

Download the following datasets and load into MySQL:

*IMDB*: https://s3.amazonaws.com/umdb-users/cjbaik/IMDB.sql 
*Yelp*: https://s3.amazonaws.com/umdb-users/cjbaik/YELP.sql
*Mondial*: in `/data` folder: `mondial-schema-mysql.sql`; `mondial-inputs-mysql.sql`

## Load Datasets

For datasets you want to use, run the following:

```
cd python
python setup_db.py <db_name>
python build_aig.py <db_name>
python tqc.py <db_name>
```

## Run Experiments

For each experiment, run:
```
cd python
python main.py <arguments>
```

*Example*: `python main.py mondial greedyall --tq_rank=equal`

Required Arguments:
- *db*: database name
- *mode*: type of algorithm to use, select from `topw`, `greedyall`, `greedybb`, `greedyfirst`, `l1s`

Optional Arguments:
- *tq_rank*: target query rank using weighting scheme described in the paper; select from `equal`, `1`, `q1` (n/4), `half` (n/2), `q3` (3n/4), `n`. Default is `equal`.
- *qid*: if you only want to run one task in the dataset, specify the ID of the task to run
- *info*: either `type` or `range`. Just use `range` which uses both information sources (data types and intersecting values) described in the paper.
- *email*: specify an email address to have the system email you when it's done with the task or if there's an error

### Logging

Logs for tasks are generated in `python/log/<db>_<mode>_tq<tq_rank>`.

### Results

After all tasks are completed, results are `pickle`d into `results/<db>_<mode>_tq<tq_rank>.pkl`.

## Analyzing Results

Analyzing results can be done using:
```
cd python
python analysis_single.py <arguments>
```

Required Arguments:
- *db*: database name
- *mode*: type of algorithm used (see list above)
- *tq_rank*: see list above

Optional Arguments:
- *qid_min*: only evaluate tasks with ID greater than or equal to this number
- *qid_max*: only evaluate tasks with ID less than or equal to this number
- *tqc_min*: only evaluate tasks with TQC value greater than this value
- *tqc_max*: only evaluate tasks with TQC value less than or equal to this value
