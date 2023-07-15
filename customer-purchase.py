from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName("Customer_Purchase")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(",")
    custNo = int(fields[0])
    orderAmount = float(fields[2])
    return custNo, orderAmount

lines = sc.textFile("data/customer-orders.csv")

orderList = lines.map(parseLine)
orderCount = orderList.reduceByKey(lambda x, y: (x+y))

flipped = orderCount.map(lambda x : (x[1], x[0]))
sortedByMaxOrder = flipped.sortByKey()
results = sortedByMaxOrder.collect()

for amount, cust in results:
    print ( "Customer ID: ",cust, " has ordered total of ", round(amount,2))