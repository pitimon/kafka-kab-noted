from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestJob") \
    .master("spark://172.31.1.121:7077") \
    .getOrCreate()

df = spark.createDataFrame([(1, "test")], ["id", "value"])
df.show()

spark.stop()