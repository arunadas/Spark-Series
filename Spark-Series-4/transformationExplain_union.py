"""
 Experimenting and recording the time taken for the 
 transformation compute compared to optimization applied.

 Source of file: https://data.gov/
 @author Aruna Das
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr , col
import os
import logging
import time

current_dir = os.path.dirname(__file__)
relative_path = "../data/SupplyChainGHGEmissionFactors_v1.2_NAICS_CO2e_USD2021.csv"
absolute_file_path = os.path.join(current_dir, relative_path)

t0 = int(round(time.time() * 1000))

# Step 1 - Creates a session on a local master
spark = SparkSession.builder.appName("Catalyst behaviour") \
    .master("local[*]").getOrCreate()

t1 = int(round(time.time() * 1000))
print("1. Creating a session ************ {}".format(t1 - t0))

# set the log level to ERROR it will remove the clutter from the console
spark.sparkContext.setLogLevel("ERROR")

# Step 2 - Reads a CSV file with header, stores it in a dataframe
df = spark.read.csv(header=True, inferSchema=True,path=absolute_file_path)

original_df = df
print("Number of orginal records .................... {}".format(original_df.count()))

t2 = int(round(time.time() * 1000))
print("2. Loading original dataset ************ {}".format(t2 - t1))

# Step 3 - Build a larger dataset
for x in range(1000):
    df = df.union(original_df)

t3 = int(round(time.time() * 1000))
print("3. Building larger dataset ************ {}".format(t3 - t2))

# Step 4 - Cleanup. preparation
df = df.withColumnRenamed("Supply Chain Emission Factors without Margins", "efwom") \
       .withColumnRenamed("Margins of Supply Chain Emission Factors", "mef")\
       .withColumnRenamed("Supply Chain Emission Factors with Margins", "efwm")

t4 = int(round(time.time() * 1000))
print("4. Clean-up complete************************ {}".format(t4 - t3))

# Step 4 - Transformation
df = df.withColumn("dmef", col("mef")) \
            .withColumn("mef2", expr("dmef * 2")) \
            .withColumn("efwm2", expr("efwom + mef2")) 

#Uncoment/comment below line 
df = df.drop("dmef","mef2","efwm2")

t5 = int(round(time.time() * 1000))
print("5. Transformations  ************ {}".format(t5 - t4))


df.show(5)
t6 = int(round(time.time() * 1000))

print("Record count of larger dataset.................... {}".format(df.count()))
print("6. Action called  ************ {}".format(t6 - t5))

# Step 5 - explain
df.explain()

spark.stop()
