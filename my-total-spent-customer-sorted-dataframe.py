from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("TotalSpentByCustomer").getOrCreate()

# load schema
schema = StructType([ 
            StructField("customerID", IntegerType(), True), 
            StructField("itemID", IntegerType(), True), 
            StructField("amountSpent", FloatType(), True)
        ])

df = spark.read.schema(schema).csv("customer-orders.csv")
grouped_by_customer = df.groupBy("customerID").agg(func.round(func.sum("amountSpent"), 2).alias("total_spent"))
sorted_by_total_spent = grouped_by_customer.sort("total_spent")

sorted_by_total_spent.show(sorted_by_total_spent.count())

spark.stop()