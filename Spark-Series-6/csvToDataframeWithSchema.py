"""
 CSV ingestion in a dataframe with a Schema.
 Source of file: https://data.gov/
 @author Aruna Das
"""
import os
import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField,
                               StringType)

current_dir = os.path.dirname(__file__)
relative_path = "../data/Street_Names.csv"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("CSV with a schema to Dataframe") \
    .master("local[*]").getOrCreate()

# set the log level to ERROR it will remove the cluter from the console
spark.sparkContext.setLogLevel("ERROR")

# Creates the schema
schema = StructType([StructField('FullStreetName', StringType(), False),
                     StructField('StreetName', StringType(), True),
                     StructField('StreetType', StringType(), False),
                     StructField('PostDirection', StringType(), True)])

# Reads a CSV file with header, called Street_Names.csv, stores it in a
# dataframe
df = spark.read.format("csv") \
    .option("header", True) \
    .option("multiline", True) \
    .option("sep", ";") \
    .schema(schema) \
    .load(absolute_file_path)


# Shows at most 10 rows from the dataframe
df.show(10, 25, False)
df.printSchema()

schemaAsJson = df.schema.json()
parsedSchemaAsJson = json.loads(schemaAsJson)

print("*** Schema as JSON: {}".format(json.dumps(parsedSchemaAsJson, indent=2)))

spark.stop()
