from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Wordcount")
sc = SparkContext(conf = conf)

input = sc.textFile("data/book.txt")
words = input.flatMap(lambda x: x.split())
wordCount = words.countByValue()

for word, count in wordCount.items():
    cleanWord = word.encode('ascii', 'ignore')
    if(cleanWord):
        print(cleanWord, count)