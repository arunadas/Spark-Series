"""
 Parquet ingestion in a dataframe.

 Source of file: util directory
 I created the parquet file using spark from csv

 @author Aruna Das
"""
from pyspark.sql import SparkSession
import os
import logging

current_dir = os.path.dirname(__file__)
relative_path = "../data/street_Names.parquet"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("Parquet to Dataframe") \
    .master("local[*]").getOrCreate()

# set the log level to ERROR it will remove the clutter from the console
spark.sparkContext.setLogLevel("ERROR")


# Reads a Parquet file, stores it in a dataframe
df = spark.read.format("parquet") \
    .load(absolute_file_path)

# Shows at most 10 rows from the dataframe
df.show(10)
df.printSchema()
print("The dataframe has {} rows.".format(df.count()))

spark.stop()