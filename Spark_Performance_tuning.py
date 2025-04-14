# Spark has two optimization engines -
a) Catalyst Optimizer - Query optimizer to produce optimized query plans.
b) Tungsten Engine - Improves efficiency of memory & CPU for Spark applications.

Catalyst Optimizer has 4 phases -

Analysis -> Logical Optimization -> Physical Planning -> Code Generation

# For large operations involving huge datasets we need to use the executors for all 'transformation' operations 
#and display results by 'Action' operations.
# DISTRIBUTED PROCESSING CHALLENGES AND SPARK PERFORMANCE TUNING.

#Usually, collect() is used to retrieve the action output when you have very small result set 
#and calling collect() on an RDD/DataFrame with a bigger result set causes out of memory as it returns the entire dataset 
#(from all workers) to the driver hence we should avoid calling collect() on a larger dataset.

#collect () vs select ()
#select() is a transformation that returns a new DataFrame and holds the columns 
#that are selected whereas collect() is an action that returns the entire data set in an Array to the driver.

#USE BROADCAST VARIABLES
-----------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from datetime import datetime, date

spark=SparkSession.builder.appName('PERFORMANCE_TUNING').getOrCreate()

df_L=spark.createDataFrame([(1,101,1001,2,30,date(2023,1,1)),(2,102,1002,1,20,date(2023,2,1)),
 (3,103,1003,5,15,date(2023,3,1)),(4,104,1004,3,30,date(2023,4,1)),
 (5,105,1005,2,20,date(2023,5,1))],schema=["TransactionID","CustomerID","ProductID","Quantity","Price","Date"] )
 
 df_S=spark.createDataFrame([(101,'Samir'),(102,'Sumit')], schema=["CustomerID","Name"])
 
 result_df=df_L.join(broadcast(df_S),"CustomerID")
result_df.show()

#PERSISTING RDD TO MEMORY AND DISK
--------------------------------------------
#Persistence (https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence)
from pyspark import StorageLevel
rdd1.persist(StorageLevel.MEMORY_AND_DISK)

#PERSISTING DATAFRAME TO MEMORY AND DISK
-----------------------------------------------

from pyspark import storagelevel
df_L.persist(storagelevel.StorageLevel.MEMORY_AND_DISK )

#SKEWNESS (uneven data distribution	across the partitions)
#a)Data skew is a very common problem with big data after shuffling and managing skew is very important for running the pipeline seamlessly.
#b) Key distribution is not uniform (highly skewed) causing some partitions to be very large and not allowing Spark to process data in parallel.

#How to mitigate skewed data ?

#SALTING --> is a technique that adds random values to the join keys , then Spark can partition data evenly.


!pip3 install pyspark
from pyspark import SparkContext,SparkConf

conf=SparkConf().setAppName("testApp1").setMaster("local[*]")
sc=SparkContext(conf=conf)

#Loading Data Skew
import numpy as np
import random

key_1= ['a'] * 10
key_2= ['b'] * 6000000
key_3=['c'] * 800
key_4=['d'] * 10000
keys=key_1 + key_2 + key_3 + key_4


values_1=list(np.random.randint(low=1, high=100, size=len(key_1)))
values_2=list(np.random.randint(low=1, high=100, size=len(key_2)))
values_3=list(np.random.randint(low=1, high=100, size=len(key_3)))
values_4=list(np.random.randint(low=1, high=100, size=len(key_4)))

values= values_1 + values_2 + values_3 + values_4

pair_skew=list(zip(keys,values))

#load data into RDD
rdd= sc.parallelize(pair_skew,8)

# Run a shuffle groupByKey() to see how the skew effects computation resources.
data_sample=[(1,4),(2,2),(2,1),(3,5),(2,5),(2,10),(2,7),(3,4),(2,1),(2,4),(4,4)]
rdd_sample=sc.parallelize(data_sample,3)

rdd_sample.glom().collect()

rdd_sample_grouped=rdd_sample.groupByKey()
#rdd_sample_grouped.collect()
# show groupby results
for item in rdd_sample_grouped.collect():
  print(item[0],[value for value in item[1]])

#show partitions
rdd_sample_grouped.glom().collect()

# This is the skewness taking more time.
grouped_rdd=rdd.groupByKey().cache()

#Run a simple data transformation using map() on the skewed data
grouped_rdd.map(lambda pair: (pair[0], [(i + 10) for i in pair[1]])).count()

# Mitigating data skewness : SALTING

def salting(val):
  tmp = val + "_" + str(random.randint(0,5))
  return tmp
  
  # Salting method
rdd_salting= rdd.map(lambda x: (salting(x [0]), x [1]))

#actual code
rdd_salting=rdd_salting.groupByKey().cache()

#run a simple data transformation using map() on the skewed data
grouped_rdd.map(lambda pair: (pair[0], [(i + 10) for i in pair[1]])).count()


# Run a shuffle sortByKey() to see how skewness effects computation resources.
rdd_sort=rdd.sortByKey(ascending=False, numPartitions=4)
rdd_sort.count()
 

