"""
 CSV ingestion in a dataframe.
 Source of file: https://data.gov/
 @author Aruna Das
"""
from pyspark.sql import SparkSession
import os
import logging

current_dir = os.path.dirname(__file__)
relative_path = "../data/Street_Names.csv"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("CSV to Dataframe") \
    .master("local[*]").getOrCreate()

# set the log level to ERROR it will remove the clutter from the console
spark.sparkContext.setLogLevel("ERROR")

print("Using Apache Spark v{}".format(spark.version))

# Reads a CSV file with header, called books.csv, stores it in a
# dataframe
df = spark.read.format("csv") \
        .option("header", "true") \
        .option("multiline", True) \
        .option("sep", ",") \
        .option("quote", "*") \
        .option("dateFormat", "MM/dd/yyyy") \
        .option("inferSchema", True) \
        .load(absolute_file_path)

print("Show dataframe content:")

# Shows at most 10 rows from the dataframe, with columns as wide as 20
# characters
df.show(10, 20)
print("Dataframe's schema:")
df.printSchema()

spark.stop()
