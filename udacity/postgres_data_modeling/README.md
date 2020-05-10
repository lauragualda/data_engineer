## Analytics Database for Sparkify

In this project we created a new database for the Analytics team at Sparkify. 
The analytics team is particularly interested in understanding what songs users are listening to, but was not able to easily access the app output data.
For that, we gathered data from user logs and songs metadata from JSON files and designed a schema in Postgres to match their specific use case.

The schema is based on the fact table `songplays` contanining high level information of each song listened by each user. From this table, analysts can understand aggregated user behavior, such as: the number of songs a user listens on a day, how many different songs an user listens on a time range. We also defined 4 dimension tables: `users`, `songs`, `artists`and `time`. 

### Structure of this repository

This repository contains the following files: 

1. Three python scripts used to maintain the Sparkify database:
  * `sql_queries.py` - Collection Postgres SQL queries to drop, create and insert records into databases
  * `create_tables.py` - Sequential commands to create database and create tables. If the database or tables already exist, they are dropped and recreated.
  * `etl.py` - ETL processes that transforms user logs and songs metadata from JSON files into the Sparkify database 
2. One jupyter notebook used for research and development of the ETL process: `etl.ipynb`
3. One jupyter notebook used to test queries during research and development phase: `test.ipynb`
4. A folder named `data`, containing user logs and song metadata in JSON files

### How to run the scripts

Download or clone this repository and make sure you run the following commands from inside your local directory where you saved it.

1. Create the Sparkify database with its 5 tables:

```python
python create_tables.py
```

2. Extract data and process data from the .json files and load them into Sparkify tables:

```python
python etl.py
```

### Analytics use cases

When joining the tables available on Sparkify, analysts will be able to easily explore the profile of users that listen to given songs or artists, look for clusters of songs and artists listened by given profiles of users and estimate which time of the day or the week users are more likely to be listening to music.

For example, an analysis on which day of the week users listen to more songs can start with the following query:

```sql
SELECT user_id, weekday, count(session_id) as n_sessions 
FROM songplays 
JOIN time USING (start_time) 
GROUP BY 1, 2
ORDER BY 1, 2 
```

And if we look at the results for a given user, like the one with `user_id=15`, we can observe that the two occasions when he listened to more than 100 songs in a day were Wednesdays (weekday 2). 

| user_id | weekday | n_sessions |
|---------|---------|------------|
| 15      | 4       | 74         |
| 15      | 5       | 27         |
| 15      | 2       | 101        |
| 15      | 4       | 9          |
| 15      | 1       | 2          |
| 15      | 2       | 18         |
| 15      | 3       | 5          |
| 15      | 0       | 10         |
| 15      | 1       | 59         |
| 15      | 2       | 103        |
| 15      | 3       | 33         |
| 15      | 0       | 22         |

Well, does that make any sense? We can now investigate looking at all the data :)
