from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("FriendsFileWithHeader").getOrCreate()

lines = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("data/fakefriends-header.csv")

lines.printSchema()
lines.select("name").show()
lines.filter(lines.age<21).show()

lines.groupby("age").count().sort('count', ascending=True).show()