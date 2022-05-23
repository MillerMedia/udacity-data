# Udacity - Data Engineering Nanodegree Program
## Project 3 - Data Warehouse

In this project, the goal is to take data, currently stored in JSON files in public Amazon S3 buckets, and migrate it to Amazon Redshift to more efficiently run analytics on it.  
  
The data is for a fictional company called Sparkify (comparable to Spotify). The datasets consist of information associated with songs and users on the Sparkify platform and songplays.

Below is the schema of the data that I have designed to ingest and analyze this data.

### How To Run
In order to run this program, follow the steps below:  
• Create an AWS Redshift cluster in region us-west-2. Make sure the cluster has the 'Publicy Accessible' option updated to 'enabled'.  
• Once created, fill in the corresponding fields in the dwh.cfg (under the [CLUSTER] heading) file with the clusters information  
• Create an IAM role in AWS. This role will need policies for S3ReadOnlyAccess and Admin RedShift access.  
• Add the ARN of the recently created role and add it under the [IAM_ROLE] heading in dwh.cfg  
• In your terminal, run `python create_tables.py`  
• In your terminal, run `python etl.py`  
• Go to the AWS console, choose your cluster and then go to the query editor where you can now connect to the database and run analytical queries directly from the GUI in AWS  

### Staging events table
#### This table has information about the 'events' that have occured. This is a table used to ingest information from the JSON files in S3 and ultimately convert and migrate it to the other tables in the schema. 

The fields and their datatypes are below. These fields were obtained based on the structure of the JSON files in S3.

    artist varchar
    auth varchar
    firstName varchar
    gender varchar
    itemInSession integer
    lastName varchar
    length numeric
    level varchar
    location varchar
    method varchar
    page varchar
    registration numeric
    sessionId int
    song varchar
    status int
    ts bigint
    userAgent varchar
    user_id int

### Staging Songs table
#### This table has information about the 'songs' on the platform. This is a table used to ingest information from the JSON files in S3 and ultimately convert and migrate it to the other tables in the schema.

The fields and their datatypes are below. These fields were obtained based on the structure of the JSON files in S3.

    num_songs int
    artist_id varchar
    artist_latitude numeric
    artist_longitude numeric
    artist_location varchar
    artist_name varchar
    song_id varchar
    title varchar
    duration numeric
    year int


(Note: The following tables follow the same schema as the previous project in this Nanodegree program)

### Songplays
#### Information about individual plays of songs

    songplay_id int identity(0,1) PRIMARY KEY
    start_time timestamp
    user_id varchar NOT NULL
    level varchar
    song_id varchar
    artist_id varchar
    session_id int
    location varchar
    user_agent varchar

### Users
#### Information about users of Sparkify

    user_id varchar PRIMARY KEY NOT NULL
    first_name varchar
    last_name varchar
    gender varchar
    level varchar

### Songs
#### Information about individual songs on Sparkify

    song_id varchar PRIMARY KEY
    title varchar
    artist_id varchar NOT NULL
    year int
    duration numeric

### Artists
#### Information about artists of songs on Sparkify

    artist_id varchar PRIMARY KEY
    name varchar NOT NULL
    location varchar
    latitude numeric
    longitude numeric

### Time
#### Breakdown of timestamps into constituent parts for easier querying

    start_time timestamp PRIMARY KEY
    hour int
    day int
    week int
    month int
    year int
    weekday int

___

## Process

Once the schemas were set and tables created, I then used the copy command to ingest JSON files from an S3 bucket and copy their contents, field by field, into the staging_events and staging_songs databases.

Once there, I created insert statements that converted the data from those tables into the songplays, users, songs, artists and time databases. These five databases will be the ones used for the subsequent analytics queries.

In theory, the data could be deleted from the staging_events and staging_songs databases once moved but I opted not to.

___

## Sample Query

#### Get the most used browsers/user agents for songplays from 10PM-11PM
`SELECT sp.user_agent, COUNT(*) FROM songplays sp JOIN time t ON sp.start_time = t.start_time WHERE t.hour = 22 GROUP BY t.hour, sp.user_agent ORDER BY COUNT(*) DESC LIMIT 5`

#### Get the most used browsers/user agents for songplays from 10PM-11PM