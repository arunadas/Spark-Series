"""
   XML ingestion to a dataframe.

   Source of file: https://data.gov/

   @author Aruna Das
"""
from pyspark.sql import SparkSession
import os
import logging



current_dir = os.path.dirname(__file__)
relative_path = "../data/Street_Names.xml"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("XML to Dataframe") \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.12.0") \
    .master("local[*]") \
    .getOrCreate()

# set the log level to ERROR it will remove the clutter from the console
spark.sparkContext.setLogLevel("ERROR")

# Reads a XML file , stores it in a dataframe
df = spark.read.format("xml") \
        .option("rowTag", "record") \
        .load(absolute_file_path)

# Shows at most 5 rows from the dataframe
df.show(10)
df.printSchema()

spark.stop()