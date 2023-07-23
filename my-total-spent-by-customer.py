from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("my_total_spent_by_customer")
sc = SparkContext(conf=conf)

input = sc.textFile("customer-orders.csv")
amount_spent = input.map(lambda x: (int(x.split(",")[0]), float(x.split(",")[2]))).reduceByKey(lambda x, y: x + y)
amount_spent_sorted = amount_spent.map(lambda x: (x[1], x[0])).sortByKey()
results = amount_spent_sorted.collect()

for result in results:
    print(str(result[1]) + ":\t\t" + str(result[0]))