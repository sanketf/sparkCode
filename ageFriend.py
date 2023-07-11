from pyspark import SparkConf, SparkContext


def parseLine(line):
    fields = line.split(",")
    age = int(fields[2])
    friends = int(fields[3])
    return age, friends


conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)

lines = sc.textFile("file:///sparkCourse/data/fakefriends.csv")
rdd = lines.map(parseLine)

totalByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

averageByAge = totalByAge.mapValues(lambda x: x[0] / x[1])

result = averageByAge.collect()

for results in result:
    print(results)
