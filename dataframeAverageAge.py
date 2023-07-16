from pyspark.sql import SparkSession

sc = SparkSession.builder.appName("DataframeRerun").getOrCreate()

data = sc.read.option("inferSchema", "true")\
    .csv("data/fakefriends.csv")

newdf = data[["_c2", "_c3"]]
newdf.groupBy('_c2').avg("_c3").sort("_c2").show()