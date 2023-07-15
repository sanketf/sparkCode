from pyspark import SparkConf, SparkContext
import re


def normilizeWord(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


conf = SparkConf().setMaster("local").setAppName("Wordcount")
sc = SparkContext(conf=conf)

inputs = sc.textFile("data/book.txt")
words = inputs.flatMap(normilizeWord)
# wordCount = words.countByValue()

wordCount = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
sortWords = wordCount.map(lambda x: (x[1], x[0]))
sortedWordCount = sortWords.sortByKey()
results = sortedWordCount.collect()

for result in results:
    count = result[0]
    word = result[1].encode('ascii', 'ignore')
    if word:
        print(word, ": ", count)
