# Data Modeling With Postgres

## **Project description:**
This project intends to attend a needance of the analytics team, 
of the Sparkfy Company, creating a Postgres Database based on the 
JSON log files they collected from songs and users activities.
The main objective of the anaytics team is to know what music the users
are listening to.

Considering the amount of data, which wasn't too big, and the kind of 
queries the analytics team want to do, the Relational Database was choosed.
And the Star Schema was choosed since this kind of schema makes easier
to do calcs and clustering, besides the fact that in this schema the use of
JOINs is simpler. We have one fact table (songplays) to multiples dimension
tables (users, songs, artists, time).

The ETL pipeline was built as follow:
First the extraction of the data from JSON log files, using the pandas module.
Then, the transformation, basically, of the datetime information.
Finally the data was loaded into the previously created postgres tables.

## **Using this Pipeline**
To run this pipeline, the first thing to do is to run the create_tables.py,
which will drop all tables existing with the choosed names, and create new 
blank tables.

```bash
python create_tables.py
```

Then run the etl.py, that will extract the data from JSON log files, that are
in the data folder, transform it and load into the tables created in the first step.

```bash
python etl.py
```

## Files in this repository:
### create_tables.py:
Responsible for drop and create all the tables, using the sql_queries.py.

### etl.ipynb:
A Jupyter Notebook file that guides us to create the ETL Process.

### etl.py:
Responsible for the ETL pipeline execution.

### sql_queries.py
Contains the commands to CREATE, DROP and INSERT into the tables, and serves
the create_tables.py.

### test.ipynb
A Jupyter Notebook file that connects to the database and do some queries,
to make sure that the data was loaded corrected.