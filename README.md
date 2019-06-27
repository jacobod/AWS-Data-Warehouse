# Sparkify Redshift Database Documentation

### 1) Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals

Sparkify runs one of the fastest growing music streaming services in the Southern California, USA market. While the streaming app gathers usage data, this data is not currently in a format or database conducive for analysis. This is a problem, as the company is not currently tracking usage statistics.

The purpose of this database is to allow for analysts and other users to be able run fast, intuitive queries on song play analysis. The database is hosted on Amazon Redshift for the following reasons:
    
    1) In the cloud. No hardware management is necessary, and redshift offers relatively large bang for buck, and being in the cloud, is scalable. 

    2) Fast(er) queries. Redshift queries on larger datasets are quicker than a regular relational database in the cloud because it uses columnar storage. This allows for analytics queries to be quicker than if using BigQuery or Azure, and fits our needs as a company dealing with 'big' music streaming data


Examples of the type of queries Sparkify would like to know include: when do listeners listen the most during the day, which arists are the most popular, and do paid listeners listen more than free-tier users?


### 2) State and justify your database schema design and ETL pipeline

#### Database Schema 

The database is organized in what is considered a STAR schema, with different 'dimension' or attribute tables (i.e. artists, songs) tied to a central "fact" table that represents a transaction important to the business (i.e. songplays).

This format ensures data integrity, as attribute fields do not appear in more than one table. This means that when data needs to be updated, it only needs to be updated in one place. 

This format also simplifies the user queries, for example to see all the users, you would only need to execute ("SELECT * FROM users") to grab the users from the song table, rather than doing ("SELECT DISTINCT user_id,first_name,last_name,level FROM songplays"), if the user data was included in the songplays table in a denormalized format.

#### ETL Pipeline

The pipeline works as so: 
    
    1) load data stored in JSON in S3, 
    2) move it to staging tables in the Redshift database
    3) move and clean data in sql to the production analytics table from the staging tables via sql
    
Keeping everything in SQL, and using python to manage queries and connect to the API provides a clean and reproducible workflow. The code used in this project functions as "Infrastructure as code"- if the database cluster goes down, or the data is corrupted, we can easily view, edit, and repeat the ETL and database creation process.

#### Repo Info and How to Run
First, create a Redshift Cluster in AWS, using the proper Security Group/IAM Roles.
Then in the project repo:

    1) update/write queries in sql_queries.py
    2) create a python console and run the whole create_tables.py file, which creates the tables for the database
    3) run the etl.py file, which loads in the data from the json files and then moves it to the analytics tables
