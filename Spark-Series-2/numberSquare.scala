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
 3. Start your scala console by executing bin/spark-shell
 4. Once the console has started execute below commands one by one in your console and observe the results
 5. For closing the console execute sbin/stop-all.sh 
 
"""

# create variable range_10 dataframe
val range_10 = spark.range(1,11).toDF("number")

# run power fun on each row of dataframe range_10 to calcate square of 1 to 10
range_10.select(pow(range_10.col("number"),2)).show()

# explain() method will show the physical plan spark will execute
range_10.select(pow(range_10.col("number"),2)).explain()