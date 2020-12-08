def main ():

    from pyspark import SparkConf
    from pyspark import SparkSession
    from pyspark.sql import DataFrame
    from pyspark import SparkContext

    conf = SparkConf() \
        .setAppName("weblogs processing") \
        .set("spark.executor.memory", "8g")
    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()








