"""
 Text ingestion in a dataframe.

 @author Aruna Das
"""
from pyspark.sql import SparkSession
import os
import logging

current_dir = os.path.dirname(__file__)
relative_path = "../data/yob2018.txt"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder \
    .appName("Text to Dataframe") \
    .master("local[*]") \
    .getOrCreate()

# set the log level to ERROR it will remove the clutter from the console
spark.sparkContext.setLogLevel("ERROR")

# Reads a Romeo and Juliet (faster than you!), stores it in a dataframe
df = spark.read.format("text") \
        .load(absolute_file_path)

# Shows at most 10 rows from the dataframe
df.show(10)
df.printSchema()

spark.stop()