from pyspark.sql import SparkSession, Row

# from pyspark import SparkConf, SparkContext

# conf = SparkConf().setMaster("local").setAppName("Friends SQL")
# sc = SparkContext(conf = conf)

spark = SparkSession.builder.appName("SQL").getOrCreate()


def lines(file):
    fields = file.split(",")
    return Row(ID=fields[0], Name=fields[1], Age=fields[2])


textFile = spark.sparkContext.textFile("data/fakefriendsql.csv")
withHeading = textFile.map(lines)

schemaPeople = spark.createDataFrame(withHeading).cache()

schemaPeople.createOrReplaceTempView("people")

teenagers = spark.sql("SELECT * FROM PEOPLE WHERE age >13 and age <19")
print("-------Output--------")
teenagers.show()

print("--------Group By ---------")
schemaPeople.groupby("Age").count().orderBy("Age").show()

spark.stop()
