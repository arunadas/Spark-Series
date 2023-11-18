"""
 Multiline ingestion JSON ingestion in a dataframe.
 Source of file: https://data.gov/
 @author Aruna Das
"""
from pyspark.sql import SparkSession
import os
import logging

current_dir = os.path.dirname(__file__)
relative_path = "../data/Street_Names.json"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("Multiline JSON to Dataframe") \
    .master("local[*]").getOrCreate()

# set the log level to ERROR it will remove the cluter from the console
spark.sparkContext.setLogLevel("ERROR")

# Reads a JSON, called countrytravelinfo.json, stores it in a dataframe
df = spark.read.format("json") \
        .option("multiline", True) \
        .load(absolute_file_path)

# Shows at most 3 rows from the dataframe
df.show(10)
df.printSchema()

spark.stop()