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

* [_**'ml-100k'**_](https://files.grouplens.org/datasets/movielens/ml-100k/) contains dataset for 'u.user', 'u.data' and 'u.item' used for this project.
* u.user contains infomation such as user_id, age, gender, occupation and zip_code.
* u.data contains infomation such as user_id, tmovie_id, trating and ttimestamp.
* u.item contains infomation such as movie_id, title, release_date, video_release_date, IMDb_URL, unknown, action, adventure, animation, children, comedy, crime, documentary, drama, fantasy, film_noir, horror, musical, mystery, romance, sci_fi, thriller, war and western.


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

Next, still in PuTTY, install Cassandra by putting together the commands below:
 
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
CREATE TABLE IF NOT EXISTS names (
    movie_id int,
    title text,
    release_date text,
    video_release_date text,
    url text,
    unknown int,
    action int,
    adventure int,
    animation int,
    children int,
    comedy int,
    crime int,
    documentary int,
    drama int,
    fantasy int,
    film_noir int,
    horror int,
    musical int,
    mystery int,
    romance int,
    sci_fi int,
    thriller int,
    war int,
    western int,
    PRIMARY KEY (movie_id)
);

-- Create the ratings table
CREATE TABLE IF NOT EXISTS ratings (
    user_id int,
    movie_id int,
    rating int,
    time int,
    PRIMARY KEY (user_id, movie_id)
);

-- Create the users table
CREATE TABLE IF NOT EXISTS users (
    user_id int,
    age int,
    gender text,
    occupation text,
    zip text,
    PRIMARY KEY (user_id)
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

def parseInput1(line):
    fields = line.split('|')
    return Row(user_id=int(fields[0]), age=int(fields[1]), gender=fields[2], occupation=fields[3], zip=fields[4])

def parseInput2(line):
    fields = line.split("\t")
    return Row(user_id=int(fields[0]), movie_id=int(fields[1]), rating=int(fields[2]), time=int(fields[3]))

def parseInput3(line):
    fields = line.split("|")
    return Row(movie_id=int(fields[0]), title=fields[1], release_date=fields[2], video_release_date=fields[3], url=fields[4],
               unknown=int(fields[5]), action=int(fields[6]), adventure=int(fields[7]), animation=int(fields[8]), 
               children=int(fields[9]), comedy=int(fields[10]), crime=int(fields[11]), documentary=int(fields[12]), 
               drama=int(fields[13]), fantasy=int(fields[14]), film_noir=int(fields[15]), horror=int(fields[16]), 
               musical=int(fields[17]), mystery=int(fields[18]), romance=int(fields[19]), sci_fi=int(fields[20]), 
               thriller=int(fields[21]), war=int(fields[22]), western=int(fields[23]))

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("CassandraIntegration") \
        .config("spark.cassandra.connection.host", "127.0.0.1") \
        .getOrCreate()

    # Load the data
    u_user = spark.sparkContext.textFile("hdfs:///user/maria_dev/wafiuddin/u.user")
    u_data = spark.sparkContext.textFile("hdfs:///user/maria_dev/wafiuddin/u.data")
    u_item = spark.sparkContext.textFile("hdfs:///user/maria_dev/wafiuddin/u.item")

    # Create RDD objects
    users = u_user.map(parseInput1)
    ratings = u_data.map(parseInput2)
    names = u_item.map(parseInput3)

    # Convert into a DataFrame
    users_df = spark.createDataFrame(users)
    ratings_df = spark.createDataFrame(ratings)
    names_df = spark.createDataFrame(names)

    # Drop the 'time' column from ratings_df to match the Cassandra schema
    ratings_df = ratings_df.drop("time")

    # Write into Cassandra
    users_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="users", keyspace="movielens") \
        .save()

    ratings_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="ratings", keyspace="movielens") \
        .save()

    names_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="names", keyspace="movielens") \
        .save()

    # Read data back from Cassandra
    readUsers = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="users", keyspace="movielens") \
        .load()

    readRatings = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="ratings", keyspace="movielens") \
        .load()

    readNames = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="names", keyspace="movielens") \
        .load()

    readUsers.createOrReplaceTempView("users")
    readRatings.createOrReplaceTempView("ratings")
    readNames.createOrReplaceTempView("names")

    # Calculate average rating for each movie
    avg_rating = spark.sql("""
        SELECT n.title, AVG(r.rating) AS avgRating 
        FROM ratings r
        JOIN names n ON r.movie_id = n.movie_id
        GROUP BY n.title
    """)

    print("Average Rating for each Movie")
    avg_rating.show(10)

    # Find the top ten movies with the highest average ratings.
    top_ten_highest = spark.sql("""
        SELECT n.title, AVG(r.rating) AS avgRating, COUNT(*) as rated_count
        FROM ratings r
        JOIN names n ON r.movie_id = n.movie_id
        GROUP BY n.title
        HAVING rated_count > 10
        ORDER BY avgRating DESC
        LIMIT 10
    """)

    print("Top 10 Movies with Highest Average Rating with More than 10 being Rated")
    top_ten_highest.show(10)

    # Find the users who have rated at least 50 movies and identify their favourite movie genres
    user_rating = spark.sql("""
        SELECT user_id, COUNT(movie_id) AS rated_count
        FROM ratings
        GROUP BY user_id
        HAVING COUNT(movie_id) >= 50
        ORDER BY user_id ASC
    """)

    user_rating.createOrReplaceTempView("user_rating")

    user_genre = spark.sql("""
        SELECT
            r.user_id,
            CASE
                WHEN n.action = 1 THEN 'Action'
                WHEN n.adventure = 1 THEN 'Adventure'
                WHEN n.animation = 1 THEN 'Animation'
                WHEN n.children = 1 THEN 'Children'
                WHEN n.comedy = 1 THEN 'Comedy'
                WHEN n.crime = 1 THEN 'Crime'
                WHEN n.documentary = 1 THEN 'Documentary'
                WHEN n.drama = 1 THEN 'Drama'
                WHEN n.fantasy = 1 THEN 'Fantasy'
                WHEN n.film_noir = 1 THEN 'Film-Noir'
                WHEN n.horror = 1 THEN 'Horror'
                WHEN n.musical = 1 THEN 'Musical'
                WHEN n.mystery = 1 THEN 'Mystery'
                WHEN n.romance = 1 THEN 'Romance'
                WHEN n.sci_fi = 1 THEN 'Sci-Fi'
                WHEN n.thriller = 1 THEN 'Thriller'
                WHEN n.war = 1 THEN 'War'
                WHEN n.western = 1 THEN 'Western'
                ELSE 'Unknown'
            END AS genre,
            SUM(r.rating) AS total_rating
        FROM ratings r
        JOIN names n ON r.movie_id = n.movie_id
        JOIN user_rating u ON r.user_id = u.user_id
        GROUP BY r.user_id, genre
        ORDER BY user_id ASC
    """)

    user_genre.createOrReplaceTempView("user_genre")

    fav_genre = spark.sql("""
        SELECT user_id, genre, total_rating
        FROM (
            SELECT user_id, genre, total_rating,
                   ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY total_rating DESC) AS row_num
            FROM user_genre
        ) AS ranked_genres
        WHERE row_num = 1
        ORDER BY user_id
    """)

    print("User's Favourite Genre")
    fav_genre.show(10)

    # All users that are less than 20 years old
    young_user = spark.sql("SELECT * FROM users WHERE age < 20")
    print("Users less than 20 years old")
    young_user.show(10)

    # All the users who have the occupation scientist and their age is between 30 and 40 years old
    scientist = spark.sql("SELECT * FROM users WHERE occupation = 'scientist' AND age BETWEEN 30 AND 40")
    print("Scientists aged between 30 and 40")
    scientist.show(10)

    # Stop spark session
    spark.stop()
```
Output:
### 1) Calculate the average rating for each movie.

Table below shows average rating for each movie.

|               title|         avgRating|
|--------------------|------------------|
|   Annie Hall (1977)| 3.911111111111111|
|Snow White and th...|3.7093023255813953|
|Paris, France (1993)|2.3333333333333335|
| If Lucy Fell (1996)|2.7586206896551726|
|    Fair Game (1995)|2.1818181818181817|
|Heavenly Creature...|3.6714285714285713|
|Night of the Livi...|          3.421875|
|         Cosi (1996)|               4.0|
| Three Wishes (1995)|3.2222222222222223|
|When We Were King...| 4.045454545454546|

### 2) Identify the top ten movies with the highest average ratings  

|               title|         avgRating|rated_count|
|--------------------|------------------|-----------|
|Close Shave, A (1...| 4.491071428571429|        112|
|Schindler's List ...| 4.466442953020135|        298|
|Wrong Trousers, T...| 4.466101694915254|        118|
|   Casablanca (1942)|  4.45679012345679|        243|
|Wallace & Gromit:...| 4.447761194029851|         67|
|Shawshank Redempt...| 4.445229681978798|        283|
|  Rear Window (1954)|4.3875598086124405|        209|
|Usual Suspects, T...| 4.385767790262173|        267|
|    Star Wars (1977)|4.3584905660377355|        583|
| 12 Angry Men (1957)|             4.344|        125|

🔶 Insights: The table above shows top 10 movies with highest average ratings.

### 3) Find the users who have rated at least 50 movies and identify their favourite movie genres.


|user_id| genre|total_rating|
|-------|------|------------|
|      1| Drama|         297|
|      2| Drama|         100|
|      3|Action|          39|
|      5|Action|         176|
|      6| Drama|         292|
|      7| Drama|         442|
|      8|Action|         159|
|     10| Drama|         259|
|     11|Comedy|         223|
|     12| Drama|          74|

🔶 Insights: The table above shows the users with minimum rated atleast 50 movies and its genres. 


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

🔶 Insights: Table above shows 10 rows of users below the age 20 years old.

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



🔶 Insights: From the dataset, the output gives us the 10 list scientist between the age 30 till 40 years old.

## Recommendations

When working with Apache Cassandra, it is essential to design your data model based on the queries you need to execute, rather than normalizing data as done in traditional relational databases. 

This approach ensures that your database schema is optimized for read performance, which is critical in distributed systems. 

Denormalization, where you duplicate data across multiple tables, is often used in Cassandra to avoid complex joins and ensure fast data retrieval. Composite keys, which combine multiple columns into a single key, can also be employed to efficiently organize and access related data. 

These strategies help to maintain high throughput and low latency, making your Cassandra deployment more effective and responsive to query demands.

## Conclusion

In conclusion, the generated CQL scripts and PySpark integration provide a robust framework for managing and querying large datasets in Apache Cassandra. By creating well-structured keyspaces and tables for users, ratings, movies, and genres, we ensure efficient data storage and retrieval. 

The integration with PySpark allows for scalable data processing and advanced analytics, such as calculating average ratings, identifying top-rated movies, and analyzing user behaviors. These operations leverage Cassandra's distributed architecture to maintain high performance and fault tolerance. 

Moreover, the careful design of data models based on query requirements, along with appropriate replication and consistency configurations, enhances the overall reliability and responsiveness of the system. This comprehensive approach enables handling complex workloads and delivering valuable insights from the data, making it well-suited for modern, data-intensive applications.
