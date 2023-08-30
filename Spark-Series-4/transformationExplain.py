"""
 CSV ingestion in a dataframe.
 Source of file: https://data.gov/
 @author Aruna Das
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr , col
import os
import logging

current_dir = os.path.dirname(__file__)
relative_path = "../data/SupplyChainGHGEmissionFactors_v1.2_NAICS_CO2e_USD2021.csv"
absolute_file_path = os.path.join(current_dir, relative_path)

# Step 1 - Creates a session on a local master
spark = SparkSession.builder.appName("Catalyst behaviour") \
    .master("local[*]").getOrCreate()

# set the log level to ERROR it will remove the clutter from the console
spark.sparkContext.setLogLevel("ERROR")

# Step 2 - Reads a CSV file with header, stores it in a dataframe
df = spark.read.csv(header=True, inferSchema=True,path=absolute_file_path)

# Step 3 - Cleanup. preparation
df = df.withColumnRenamed("Supply Chain Emission Factors without Margins", "efwom") \
       .withColumnRenamed("Margins of Supply Chain Emission Factors", "mef")\
       .withColumnRenamed("Supply Chain Emission Factors with Margins", "efwm")

# Step 4 - Transformation
df = df.withColumn("dmef", col("mef")) \
            .withColumn("mef2", expr("dmef * 2")) \
            .withColumn("efwm2", expr("efwom + mef2")) 

df.show(5)

# Step 5 - explain
df.explain()

spark.stop()
