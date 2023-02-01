import mmh3
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as fun
from pyspark.sql.functions import when 
from pyspark.sql import Window as W
from pyspark.sql import types as T
from pyspark import SparkConf
from pyspark import SparkContext as sc

spark = SparkSession.builder.master("local[*]").getOrCreate()
sparkconf = SparkConf().set('spark.sql.shuffle.partitions',24)

Schema = StructType([
  StructField('UserName',StringType(),nullable=True),
  StructField('MovieName',StringType(),nullable=True),
  StructField('MovieType',StringType(),nullable=True),
  StructField('MovieTags',StringType(),nullable=True),
  StructField('Rating',StringType(),nullable=True)])

movies = spark.read.option("header",False).schema(Schema).csv("data/out.csv")
movies = movies.drop('MovieType','MovieTags')

# print("============================")
# print("nombre de ligne :" + str(movies.count()))
# print("============================")
#movies.na.drop("all").show(truncate=False)
#Removing all the rows with null (incomplete)
#map reduce : map in each node, reduce is results, traitement in each node to optimize

movies2 = movies.filter((fun.col("UserName") != fun.lit("null"))& 
                        (fun.col("MovieName")!= fun.lit("null"))&
                        (fun.col("Rating")   != fun.lit("null")))

#what cariable codes for null 
#Converting all the dataframe into uppercase

movies2 = movies2.withColumn("m_User",fun.upper(fun.col("UserName"))).drop("UserName")
movies2 = movies2.withColumn("m_MovieName",fun.upper(fun.col("MovieName"))).drop("MovieName")
# movies2 = movies2.withColumn("m_MovieType",fun.upper(fun.col("MovieType"))).drop("MovieType")
# movies2 = movies2.withColumn("m_MovieTags",fun.upper(fun.col("MovieTags"))).drop("MovieTags")
movies2 = movies2.withColumn("m_Rating",fun.upper(fun.col("Rating"))).drop("Rating")

#Sorting the data frame alphabeticaly

#Removing the duplicated rows from the data frame
movies3 = movies2.dropDuplicates(['m_User','m_MovieName']).sort("m_User")
print("number of lines" + str(movies3.count()))
movies4 = movies3.withColumn("ratings",
when(movies3.m_Rating == "VERY BAD",0).
when(movies3.m_Rating == "BAD",1).
when(movies3.m_Rating == "AVERAGE",2).
when(movies3.m_Rating == "GOOD",3).
when(movies3.m_Rating == "VERY GOOD",4).
when(movies3.m_Rating == "EXCELLENT",5).
when(movies3.m_Rating == "VERY BAD;BAD",0.5).
when(movies3.m_Rating == "BAD;AVERAGE",1.5).
when(movies3.m_Rating == "AVERAGE;GOOD",2.5).
when(movies3.m_Rating == "GOOD;VERY GOOD",3.5).
when(movies3.m_Rating == "VERY GOOD;EXCELLENT",4.5).
otherwise(None)).drop("m_Rating")
movies4 = movies4.withColumn("num_row",fun.monotonically_increasing_id())
movies4.filter(fun.col("ratings").isNotNull()).drop()

# print("after the ratings:" + str(movies4.count())
# movies4.na.drop(susbet=["ratings"]).count()

unique_usernames = movies4.select("m_User").distinct().rdd.map(lambda x: x[0]).collect()
sorted_usernames = sorted(unique_usernames)
username_id_dict = {username: i+1 for i, username in enumerate(sorted_usernames)}
# Convert the dictionary to a PySpark dataframe
id_df = spark.createDataFrame(list(username_id_dict.items()), ["m_User", "user_id"])
aux = movies4.join(fun.broadcast(id_df), movies4["m_User"] == id_df["m_User"], "inner").drop("m_user","ratings")

print("==============================")
# print("nombre de ligne d'aux:" + str(aux.count()))
# aux.show()

unique_movie = movies4.select("m_MovieName").distinct().rdd.map(lambda x: x[0]).collect()
sorted_movies = sorted(unique_movie)
movies_id_dict = {movie_name: i+1 for i, movie_name in enumerate(sorted_movies)}
id_df = spark.createDataFrame(list(movies_id_dict.items()), ["m_MovieName", "movie_id"])
aux2 = movies4.join(fun.broadcast(id_df), movies4["m_MovieName"] == id_df["m_MovieName"], "inner").drop("m_MovieName")
# print("nombre de ligne d'aux2:" + str(aux2.count()))
print("=============================")
res = aux.join(aux2,aux.num_row == aux2.num_row,"").select("user_id","movie_id","ratings")
print("final count : " + str(res.count()))
# aux2.show()
# movie_final = movies4.select("user_id","movie_id","ratings")
# movies3.withColumn('id',fun.dense_rank())

# movies3.write.csv("data/output_test")
#movies3.show(40)
# print("============================")
# print("nombre de ligne :" + str(movies3.count()))
# print("============================")
