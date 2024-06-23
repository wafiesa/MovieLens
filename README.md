# Cassandra Query Language on MovieLens Dataset 

## Project Overview 

This project proposes to perform Cassandra Query Language (CQL) on MovieLens 100k (ml-100k) dataset to:
* calculate the average rating for each movie.
* identify the top ten movies with the highest average ratings.  
* find the users who have rated at least 50 movies and identify their favourite movie genres.
* find all the users with age that is less than 20 years old.
* find all the users who have the occupation “scientist” and their age is between 30 and 40 years old.

![GroupLens.jpeg](https://drive.google.com/uc?export=view&id=1md8ejKluIY0mG7G3k1TVtJRK04nECz-I)

**Image 1: GroupLens**
**photo credit to owner**

## Cassandra

Apache Cassandra is a highly scalable, distributed NoSQL database designed to handle large amounts of data across many commodity servers with no single point of failure. 

Developed initially by Facebook and now an Apache Software Foundation project, Cassandra excels in managing structured data that can grow to massive scales. Its architecture allows for continuous availability, linear scalability, and fault tolerance. 

With support for multi-datacenter replication, Cassandra is well-suited for applications requiring high availability and reliability. Its schema-free design, coupled with a powerful query language (CQL), makes it versatile for various use cases, from real-time analytics to large-scale transactional systems.

## Code and Resources Used

* Hortonworks HDP Sandbox Version: 2.6.5.0
* Putty Version: 0.81
* Cassandra Version: 3.0.9
* Spark2 Version: 2.3.0

## Dataset Information

* [_**'ml-100k'**_](https://files.grouplens.org/datasets/movielens/ml-100k/) contains dataset og 'u.user', 'u.data' and 'u.item' used for this project.

#### Load The Dataset

The dataset can be uploaded to HDFS using the command below:

#### PuTTY Commands

```
wget https://files.grouplens.org/datasets/movielens/ml-100k/u.user
wget https://files.grouplens.org/datasets/movielens/ml-100k/u.data
wget https://files.grouplens.org/datasets/movielens/ml-100k/u.item

hadoop fs -copyFromLocal u.user /Name/u.user
hadoop fs -copyFromLocal u.data /Name/u.data
hadoop fs -copyFromLocal u.item /Name/u.item
```
Once the dataset has been uploaded, we can check the at the Ambari by selecting Fileview's tab.

Next in, still in PuTTY, install Cassandra by putting together the commands below:
 
#### Install Cassandra

```
su root

cd /etc/yum.repos.d

vi datastax.repo
[datastax]
name=DataStax Repo for Apache Cassandra
baseurl=http://rpm.datastax.com/community
enabled=1
gpgcheck=0

yum install dsc30

service cassandra start
```
The last line of the command above will initiate Cassandra.

Then, we continue to create database using Cassandra Query Language (CQL).

```
cqlsh

-- Create the keyspace
CREATE KEYSPACE movielens WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Create the users table
CREATE TABLE movielens.users (
    user_id int PRIMARY KEY,
    age int,
    gender text,
    occupation text,
    zip text
);

-- Create the ratings table
CREATE TABLE movielens.ratings (
    user_id int,
    movie_id int,
    rating int,
    timestamp bigint,
    PRIMARY KEY (user_id, movie_id)
);

-- Create the movies table
CREATE TABLE movielens.movies (
    movie_id int PRIMARY KEY,
    title text,
    genres list<text>
);

-- Create the movie names
CREATE TABLE IF NOT EXISTS movielens.movie_names (
    movie_id int PRIMARY KEY,
    title text
);

exit
```
Next, we will create Cassandra_MovieLens.py using the command below:

#### Create Cassandra_MovieLens.py 
In PuTTY environment, create a script (vi Cassandra_MovieLens.py) to depict python code in Spark environment as below: 

```
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F

def parseUserInput(line):
    fields = line.split('|')
    return Row(user_id=int(fields[0]), age=int(fields[1]), gender=fields[2], occupation=fields[3], zip=fields[4])

def parseRatingInput(line):
    fields = line.split()
    return Row(user_id=int(fields[0]), movie_id=int(fields[1]), rating=int(fields[2]), timestamp=int(fields[3]))

def parseMovieInput(line):
    fields = line.split('|')
    genres = fields[5:]  # Assuming genres start from the 6th field onwards
    return Row(movie_id=int(fields[0]), title=fields[1], genres=genres)

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("CassandraIntegration") \
        .config("spark.cassandra.connection.host", "127.0.0.1") \
        .getOrCreate()

    # Load user data
    user_lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/wafiuddin/u.user")
    users = user_lines.map(parseUserInput)
    usersDataset = spark.createDataFrame(users)
    usersDataset.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="users", keyspace="movielens") \
        .save()

    # Load rating data
    rating_lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/wafiuddin/u.data")
    ratings = rating_lines.map(parseRatingInput)
    ratingsDataset = spark.createDataFrame(ratings)
    ratingsDataset.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="ratings", keyspace="movielens") \
        .save()

    # Load movie data
    movie_lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/wafiuddin/u.item")
    movies = movie_lines.map(parseMovieInput)
    moviesDataset = spark.createDataFrame(movies)
    moviesDataset.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="movies", keyspace="movielens") \
        .save()

    # Read data back from Cassandra
    usersDF = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="users", keyspace="movielens") \
        .load()

    ratingsDF = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="ratings", keyspace="movielens") \
        .load()

    moviesDF = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="movies", keyspace="movielens") \
        .load()

    # Calculate average rating for each movie
    avg_ratingsDF = ratingsDF.groupBy("movie_id").agg(F.avg("rating").alias("average_rating"))

    # Find the top ten highest average ratings
    top_ten_moviesDF = avg_ratingsDF.orderBy(F.desc("average_rating")).limit(10)

    # Find users who have rated at least 50 movies and their favorite movie genres
    user_ratings_countDF = ratingsDF.groupBy("user_id").count().alias("movie_count")
    active_usersDF = user_ratings_countDF.filter(user_ratings_countDF['count'] >= 50)
    favorite_genresDF = ratingsDF.join(moviesDF, "movie_id") \
        .groupBy("user_id", "genres").count() \
        .orderBy("user_id", F.desc("count"))

    # Find users under 20 years old
    young_usersDF = usersDF.filter(usersDF.age < 20)

    # Find users who are scientists and aged between 30 and 40
    scientist_usersDF = usersDF.filter((usersDF.occupation == "scientist") & (usersDF.age.between(30, 40)))

    # Display results
    avg_ratingsDF.show()
    top_ten_moviesDF.show()
    favorite_genresDF.show()
    young_usersDF.show()
    scientist_usersDF.show()

    spark.stop()
```
Output:
### 1) Calculate the average rating for each movie.

Table below shows the top 20 rows for average rating for each movie.

|movie_id|    average_rating|
|--------|------------------|
|     471|3.6108597285067874|
|     496| 4.121212121212121|
|     148|          3.203125|
|     833| 3.204081632653061|
|     463| 3.859154929577465|
|    1088| 2.230769230769231|
|    1591|3.1666666666666665|
|    1238|             3.125|
|    1645|               4.0|
|    1580|               1.0|
|    1342|               2.5|
|    1522|2.4285714285714284|
|     540| 2.511627906976744|
|     243|2.4393939393939394|
|    1084| 3.857142857142857|
|    1025|2.9318181818181817|
|     392|3.5441176470588234|
|     623| 2.923076923076923|
|     737| 2.983050847457627|
|    1483|3.4166666666666665|

### 2) Identify the top ten movies with the highest average ratings  

|movie_id|average_rating|
|--------|--------------|
|    1599|           5.0|
|    1293|           5.0|
|    1653|           5.0|
|    1201|           5.0|
|    1189|           5.0|
|    1467|           5.0|
|    1122|           5.0|
|    1500|           5.0|
|    1536|           5.0|
|     814|           5.0|

🔶 Insights: The table above shows top 10 movies with highesr average ratings.

### 3) Find the users who have rated at least 50 movies and identify their favourite movie genres.


|user_id|              genres|count|
|-------|--------------------|-----|
|      1|[0, 0, 0, 0, 0, 0...|   39|
|      1|[0, 0, 0, 0, 0, 1...|   24|
|      1|[0, 0, 0, 0, 0, 1...|   13|
|      1|[0, 0, 0, 0, 0, 0...|   11|
|      1|[0, 0, 0, 0, 1, 1...|    8|
|      1|[0, 1, 1, 0, 0, 0...|    7|
|      1|[0, 1, 0, 0, 0, 0...|    7|
|      1|[0, 0, 0, 0, 0, 1...|    7|
|      1|[0, 0, 0, 0, 0, 0...|    7|
|      1|[0, 0, 0, 0, 0, 0...|    6|
|      1|[0, 0, 0, 0, 0, 0...|    5|
|      1|[0, 0, 0, 0, 0, 1...|    4|
|      1|[0, 1, 1, 0, 0, 0...|    4|
|      1|[0, 0, 0, 0, 0, 0...|    3|
|      1|[0, 0, 0, 0, 0, 1...|    3|
|      1|[0, 0, 0, 0, 0, 0...|    3|
|      1|[0, 1, 1, 0, 0, 0...|    3|
|      1|[0, 0, 0, 0, 0, 0...|    3|
|      1|[0, 1, 0, 0, 0, 0...|    3|
|      1|[0, 1, 0, 0, 0, 0...|    3|

only showing top 20 rows

🔶 Insights: The table above shows the user with minimum rated atleast 50 movies. 
The table also indicates the genres 0 and 1 which represent for Unkown and Action movies respectively.  

### 4) Find all the users with age that is less than 20 years old.


|user_id|age|gender|   occupation|  zip|
|-------|---|------|-------------|-----|
|    320| 19|     M|      student|24060|
|    461| 15|     M|      student|98102|
|     67| 17|     M|      student|60402|
|    642| 18|     F|      student|95521|
|    588| 18|     F|      student|93063|
|     30|  7|     M|      student|55436|
|    528| 18|     M|      student|55104|
|    674| 13|     F|      student|55337|
|    375| 17|     M|entertainment|37777|
|    851| 18|     M|        other|29646|
|    859| 18|     F|        other|06492|
|    813| 14|     F|      student|02136|
|     52| 18|     F|      student|55105|
|    397| 17|     M|      student|27514|
|    257| 17|     M|      student|77005|
|    221| 19|     M|      student|20685|
|    368| 18|     M|      student|92113|
|    507| 18|     F|       writer|28450|
|    262| 19|     F|      student|78264|
|    880| 13|     M|      student|83702|

only showing top 20 rows

🔶 Insights: Table above shows top 20 rows of users below the age 20 years old.

### 5) Find all the users who have the occupation “scientist” and their age is between 30 and 40 years old.


|user_id|age|gender|occupation|  zip|
|-------|---|------|----------|-----|
|    538| 31|     M| scientist|21010|
|    183| 33|     M| scientist|27708|
|    107| 39|     M| scientist|60466|
|    918| 40|     M| scientist|70116|
|    337| 37|     M| scientist|10522|
|    554| 32|     M| scientist|62901|
|     40| 38|     M| scientist|27514|
|     71| 39|     M| scientist|98034|
|    643| 39|     M| scientist|55122|
|    543| 33|     M| scientist|95123|
|    730| 31|     F| scientist|32114|
|     74| 39|     M| scientist|T8H1N|
|    272| 33|     M| scientist|53706|
|    430| 38|     M| scientist|98199|
|    874| 36|     M| scientist|37076|
|    309| 40|     M| scientist|70802|


🔶 Insights: From the dataset, the output gives us 16 scientist between the age 30 till 40 years old.

## Recommendations

When working with Apache Cassandra, it is essential to design your data model based on the queries you need to execute, rather than normalizing data as done in traditional relational databases. 

This approach ensures that your database schema is optimized for read performance, which is critical in distributed systems. 

Denormalization, where you duplicate data across multiple tables, is often used in Cassandra to avoid complex joins and ensure fast data retrieval. Composite keys, which combine multiple columns into a single key, can also be employed to efficiently organize and access related data. 

These strategies help to maintain high throughput and low latency, making your Cassandra deployment more effective and responsive to query demands.

## Conclusion

In conclusion, the generated CQL scripts and PySpark integration provide a robust framework for managing and querying large datasets in Apache Cassandra. By creating well-structured keyspaces and tables for users, ratings, movies, and genres, we ensure efficient data storage and retrieval. 

The integration with PySpark allows for scalable data processing and advanced analytics, such as calculating average ratings, identifying top-rated movies, and analyzing user behaviors. These operations leverage Cassandra's distributed architecture to maintain high performance and fault tolerance. 

Moreover, the careful design of data models based on query requirements, along with appropriate replication and consistency configurations, enhances the overall reliability and responsiveness of the system. This comprehensive approach enables handling complex workloads and delivering valuable insights from the data, making it well-suited for modern, data-intensive applications.
