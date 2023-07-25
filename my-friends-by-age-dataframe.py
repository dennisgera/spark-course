from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("friends_by_age").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("fakefriends-header.csv")

friends_by_age = people.select("age", "friends")