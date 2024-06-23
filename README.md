# <div align="center"> MovieLens Data Analysis with MongoDB and Cassandra 🎥</div>

## <div align="center">![Intro](images/muvielens.png)

## Introduction

This project presents an analysis of the MovieLens 100k dataset using Apache Spark integrated with MongoDB and Cassandra. The dataset includes user information, movie ratings, and movie details, providing a comprehensive basis for exploring user preferences and movie popularity.

## Objectives
The main objectives of this analysis are:
1. Calculate the average rating for each movie.
2. Identify the top ten movies with the highest average ratings.
3. Find the users who have rated at least 50 movies and identify their favorite movie genres.
4. Find all the users with age less than 20 years old.
5. Find all the users who have the occupation "scientist" and are between 30 and 40 years old.

## Coding Information

I have answered the following questions using different databases:

- Questions i, ii, and iii: MongoDB

- Questions iv and v: Cassandra (our gossiper)

Hence, you will see the coding divided into two sections: MongoDB and Cassandra. The full code for both databases will be included in the index section.

## Python 🐍 Script Elements

### 1. Python Libraries Used
The following Python libraries are used to execute Spark, MongoDB, and Cassandra sessions:
```python
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F
```

### 2. Functions to Parse the u.data, u.item, and u.user Files
Functions to parse the u.data, u.item, and u.user files and load them into HDFS:
```python
def parseUserInput(line):
    fields = line.split('|')
    return Row(user_id=int(fields[0]), age=int(fields[1]), gender=fields[2], occupation=fields[3], zip=fields[4])

def parseRatingInput(line):
    fields = line.split('\t')
    return Row(user_id=int(fields[0]), movie_id=int(fields[1]), rating=int(fields[2]), timestamp=int(fields[3]))

def parseMovieInput(line):
    fields = line.split('|')
    return Row(movie_id=int(fields[0]), title=fields[1], genres=fields[5:])
```

### 3. Functions to Load, Read, and Create RDD Objects
Functions to load the data from HDFS, read it, and create Resilient Distributed Dataset (RDD) objects:

MongoDB:
```python
if __name__ == "__main__":
    spark = SparkSession.builder.appName("MongoIntegration").getOrCreate()

    # Load and parse the ratings data
    rating_lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/azlin/u.data")
    ratings = rating_lines.map(parseRatingInput)
    ratingsDF = spark.createDataFrame(ratings)

    # Load and parse the movies data
    movie_lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/azlin/u.item")
    movies = movie_lines.map(parseMovieInput)
    moviesDF = spark.createDataFrame(movies)
```
Cassandra:
```python
if __name__ == "__main__":
    spark = SparkSession.builder.appName("CassandraIntegration")\
        .config("spark.cassandra.connection.host", "127.0.0.1")\
        .getOrCreate()
    
    # Load and parse the user data
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/azlin/u.user")
    users = lines.map(parseInput)
    usersDataset = spark.createDataFrame(users)
```

### 4. Functions to Convert the RDD Objects into DataFrame
Convert the RDD objects into DataFrames:

MongoDB:
```python
    ratingsDF = spark.createDataFrame(ratings)
    moviesDF = spark.createDataFrame(movies)
```

Cassandra:
```python
    usersDataset = spark.createDataFrame(users)
```
### 5. Functions to Write the DataFrame into the Keyspace Database in MongoDB and Cassandra
Write the DataFrames into the Keyspace database created in MongoDB and Cassandra:

MongoDB:
```python
    # Write DataFrames into MongoDB
    # Write ratings data into MongoDB
    ratingsDF.write.format("com.mongodb.spark.sql.DefaultSource")\
        .option("uri", "mongodb://127.0.0.1/movielens.ratings").mode('append').save()

    # Write movies data into MongoDB
    moviesDF.write.format("com.mongodb.spark.sql.DefaultSource")\
        .option("uri", "mongodb://127.0.0.1/movielens.movies").mode('append').save()
```

Cassandra:
```python
    # Write users data into Cassandra
    usersDataset.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table="users", keyspace="movielens")\
        .save()
```

### 6. Functions to Read the Table Back from MongoDB and Cassandra into a New DataFrame
Read the tables back from MongoDB and Cassandra into new DataFrames:

MongoDB:
```python
    # Read DataFrames back from MongoDB
    # Read ratings data from MongoDB
    ratingsDF = spark.read.format("com.mongodb.spark.sql.DefaultSource")\
        .option("uri", "mongodb://127.0.0.1/movielens.ratings").load()

    # Read movies data from MongoDB
    moviesDF = spark.read.format("com.mongodb.spark.sql.DefaultSource")\
        .option("uri", "mongodb://127.0.0.1/movielens.movies").load()

    # Create Temp Views for SQL queries
    ratingsDF.createOrReplaceTempView("ratings")
    moviesDF.createOrReplaceTempView("movies")
```

Cassandra:
```python
    # Read users data from Cassandra
    readUsers = spark.read\
        .format("org.apache.spark.sql.cassandra")\
        .options(table="users", keyspace="movielens")\
        .load()

    readUsers.createOrReplaceTempView("users")

```

## Questions and Answers

### MongoDB Analysis

i) Calculate the average rating for each movie.
```python
    # Question (i): Calculate the average rating for each movie
    avg_ratings = spark.sql("""
        SELECT movie_id, AVG(rating) as avg_rating
        FROM ratings
        GROUP BY movie_id
    """)
    avg_ratings.show(10)
```
![1](images/1.png)

ii) Identify the top ten movies with the highest average ratings.
```python
    # Question (ii): Identify the top ten movies with the highest average ratings
    top_ten_movies = avg_ratings.join(moviesDF, "movie_id")\
        .select("title", "avg_rating")\
        .orderBy(F.desc("avg_rating"))\
        .limit(10)
    top_ten_movies.show()
```
![2](images/2.png)

iii) Find the users who have rated at least 50 movies and identify their favourite movie genres
```python
    # Question (iii): Find the users who have rated at least 50 movies and identify their favorite movie genres
    users_50_ratings = spark.sql("""
        SELECT user_id, COUNT(movie_id) as movie_count
        FROM ratings
        GROUP BY user_id
        HAVING movie_count >= 50
    """)
    users_50_ratings.createOrReplaceTempView("users_50_ratings")

    # Explode genres and create a new DataFrame
    exploded_movies = moviesDF.withColumn("genre", F.explode("genres"))
    exploded_movies.createOrReplaceTempView("exploded_movies")

    favorite_genres = spark.sql("""
        SELECT u.user_id, e.genre, COUNT(e.movie_id) as genre_count
        FROM users_50_ratings u
        JOIN ratings r ON u.user_id = r.user_id
        JOIN exploded_movies e ON r.movie_id = e.movie_id
        GROUP BY u.user_id, e.genre
        ORDER BY u.user_id, genre_count DESC
    """)
    favorite_genres.show(10)

    # Stop the session
    spark.stop()
```
![3](images/3.png)
