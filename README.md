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
```python
if __name__ == "__main__":
    spark = SparkSession.builder.appName("DataIntegration")\
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/movielens")\
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/movielens")\
        .config("spark.cassandra.connection.host", "127.0.0.1")\
        .getOrCreate()

    user_lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/azlin/u.user")
    users = user_lines.map(parseUserInput)

    rating_lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/azlin/u.data")
    ratings = rating_lines.map(parseRatingInput)

    movie_lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/azlin/u.item")
    movies = movie_lines.map(parseMovieInput)
```

### 4. Functions to Convert the RDD Objects into DataFrame
Convert the RDD objects into DataFrames:
```python
    usersDF = spark.createDataFrame(users)
    ratingsDF = spark.createDataFrame(ratings)
    moviesDF = spark.createDataFrame(movies)
```
### 5. Functions to Write the DataFrame into the Keyspace Database in MongoDB and Cassandra
Write the DataFrames into the Keyspace database created in MongoDB and Cassandra:
```python
    usersDF.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table="users", keyspace="movielens")\
        .save()

    ratingsDF.write\
        .format("com.mongodb.spark.sql.DefaultSource")\
        .mode('append')\
        .option("uri", "mongodb://127.0.0.1/movielens.ratings")\
        .save()

    moviesDF.write\
        .format("com.mongodb.spark.sql.DefaultSource")\
        .mode('append')\
        .option("uri", "mongodb://127.0.0.1/movielens.movies")\
        .save()
```
