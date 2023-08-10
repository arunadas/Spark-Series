"""
Name : Number Square of range of numbers from 1 to 10
Date : 08/09/2023
Author : Aruna Das

"""

"""
 Instruction to open spark python console
 Locate sbin directory in your apache version of standalone spark

 Follow below steps:
 1. Start the spark by executing sbin/start-all.sh
 2. Issue 'jps' command on your termial you should see Master , Worker processes of Spark runing
 3. Start your pyspark console by executing bin/pyspark
 4. Once the console has started execute below commands one by one in your console and observe the results
 5. For closing the console execute sbin/stop-all.sh

"""

# Create dataframe of intergers from 1 to 10 and name the column number
#  you have a dataframe called range_10 now
range_10 = spark.range(1,11).toDF("number")

# select column number and apply power function to calcate square 
#  show() methos is a action which triggres Spark to execute
range_10.select(range_10["number"] ** 2).show()

# explain() method will show the physical plan spark will execute
range_10.select(range_10["number"] ** 2).explain()

# For SQL part create a view named range10 and run your SQL in same console
range_10.createOrReplaceTempView("range10")

usingSQL = spark.sql(""" SELECT POWER(number,2)
... FROM range10""")
usingSQL.show()

# explain() method will show the physical plan spark will execute
usingSQL.explain()