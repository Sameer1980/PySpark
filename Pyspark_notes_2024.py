#PySpark is a powerful tool for processing large datasets. Two key concepts in PySpark are `SparkSession` and `SparkContext`.

#What is SparkSession?
#SparkSession` is the entry point to programming with Spark. 
#It provides a single point of entry to interact with Spark functionality and to create DataFrame and DataSet.

#Why use SparkSession?
#1. Unified Interface: It combines the old `SQLContext`, `HiveContext`, and `SparkContext` into a single point of entry.
#2. Ease of Use: Simplifies the process of starting and configuring a Spark application.

#Here’s how to create a `SparkSession` in PySpark:
#=========================================================
from pyspark.sql import SparkSession

#Create a SparkSession
spark = SparkSession.builder \
.appName(“example_spark_session”) \
.getOrCreate()

#In this example:
#- `SparkSession.builder` starts the process of building a `SparkSession`.
#- `appName(“example_spark_session”)` sets a name for your application.
#- `getOrCreate()` either retrieves an existing `SparkSession` or creates a new one.

#What is SparkContext?
#SparkContext` is the original entry point for Spark functionality.
 #It’s responsible for connecting to the Spark cluster, loading data, and interacting with Spark’s core functionalities.
 
#Why use SparkContext?
#1. Low-level Operations: Provides access to low-level operations and configurations.
#2. RDD Manipulation: Essential for working directly with Resilient Distributed Datasets (RDDs).

#============================================================================================================================
#If you're working with Hive, you can enable Hive support using the enableHiveSupport method. 
#This provides a Spark Session with Hive support, including connectivity to a persistent Hive metastore, 
#support for Hive SerDes, and Hive user-defined functions (UDFs).

spark = SparkSession.builder \

 .appName("Spark Session Example") \

 .enableHiveSupport() \

 .getOrCreate()

#================================================================================================================================
#You can set the location of the Spark warehouse,
# which is the directory where Spark will store table data, using the config method with the "spark.sql.warehouse.dir" property.

spark = SparkSession.builder \

 .appName("Spark Session Example") \

 .config("spark.sql.warehouse.dir", "/path/to/warehouse") \

 .getOrCreate()

#================================================================================================================================

#Here’s how to create a `SparkContext` in PySpark:
#==================================================================
from pyspark import SparkContext, SparkConf
#Create a Spark configuration
conf = SparkConf().setAppName(“example_spark_context”)

#Create a SparkContext
#sc = SparkContext(conf=conf)

#In this example:
#- `SparkConf().setAppName(“example_spark_context”)` sets the application name in the Spark configuration.
#- `SparkContext(conf=conf)` initializes a `SparkContext` with the given configuration.

#Where to Use What and Why?
#1. SparkSession:
#— Use `SparkSession` when working with high-level APIs such as DataFrame and SQL.
#— Ideal for most data processing tasks because of its simplicity and unified interface.

#2. SparkContext:
#— Use `SparkContext` for low-level RDD operations.
#— Necessary when you need more control over the underlying Spark execution or need to manipulate RDDs directly.


# Spark Coding Environment
#Install PySpark using PIP3 in Google Colab.
# SparkConf().setMaster("local[2]") is the number of cores available.
#What does Spark master local [*] mean?
#The --master option specifies the master URL for a distributed cluster, or local to run locally with one thread,
 #or local[N] to run locally with N threads. You should start by using local for testing.
 #And from here: local[*] Run Spark locally with as many worker threads as logical cores on your machine.
 
 #setMaster() -denotes where to run your spark application local or cluster. When you run on a cluster, 
 #you need to specify the address of the Spark master or Driver URL for a distributed cluster.
 #We usually assign a local[*] value to setMaster() in spark while doing internal testing.
 
 # IMPORTANT KEYWORDS:-
 #a) Master or Driver : Heart of the spark application. Run’s your main() function and analyze the code
 #and executor’s required to complete this work and distributes the work to Slaves(executor’s) in terms of Task.
 
# b) Cluster Manager: A cluster or group of machines, pools the resources of many machines together allowing 
#us to use all the cumulative resources as if they were one. It provides executor resources as requested by the Master or Driver.

#c) Executor or Slave: The executors are responsible for carrying out the work that the driver assigns them 
#and reporting the state of the computation on that executor back to driver.

#So on a high level, the master in spark analyzes the work that is needed and the executor will complete that work assigned to it by the master.

# d)    Task: It is the piece of code that an executor gets to run. Cores are the maximum number of tasks an executor can run in parallel.

#e) Slot: It is sometimes referred to as the number of cores available per executor. Although slots are often referred to as cores in Spark,
# they’re implemented as threads that work on a physical core’s thread and don’t need to correspond to the number of physical CPU cores on the machine
# (since different CPU manufacturers can architect multi-threaded chips differently). 

#f) Threads: are the virtual components or codes, which divide the physical core of a CPU into multiple virtual cores.

# A task is a piece of code that runs on the executor. Simply, in spark one Task is created for processing one partition
#1 Task = 1 Partition
#By creating Tasks, the Driver can assign units of work to Slots on each Executor for parallel execution.

#Number of Cores per executor = Number of Tasks it can run parallel per executor.
#Here in our case, each executor can run 2 tasks
#1 Executor = 2 Cores = 2 Tasks (in parallel)

#Most processors of today are multi-threaded. If your CPU splits 1 physical core into 2 virtual cores i.e. 2 threads
#1 Executor = 2 Cores = 4 Threads

# So overall, each Executor can run n Partitions(Tasks) in parallel using n cores available per executor.

# builder.appName(name: str) → pyspark.sql.session.SparkSession.Builder 
#Sets a name for the application, which will be shown in the Spark web UI.
#If no application name is set, a randomly generated name will be used.

#What are the three Execution modes in Spark?
#The Spark framework has three execution modes.

#a)Client mode((Interactive Mode)
#In the Client mode, Spark Driver remains on the Client machine that submits the Spark application.
#The driver runs on the client machine or gateway node.
# All the executors run in the Spark cluster. The downside of this is that there can latency issues here.
#This mode is suitable for interactive and debugging purposes.

#b)Cluster mode(Non-interactive Mode)
#In the Cluster mode, Spark Driver runs on one of the worker nodes within the Spark cluster.
#The driver runs on a random node in the cluster.
# This removes the dependency on the client and lowers the latency issues.
#This mode is suitable for running applications in production.

#c)Local mode
#In the Local mode, the Spark Driver and executors run on a single machine. 
#This is purely for testing purposes as it is not feasible to achieve distribution on a single machine.
 
!pip3 install pyspark
from pyspark import SparkContext,SparkConf

conf=SparkConf().setAppName("testApp1").setMaster("local[*]")
sc=SparkContext(conf=conf)

# To print out the spark context
print (sc)

# To print out the number of cores.
sc.defaultParallelism

# For RDD we have two operations -> 1) Transformations , 2) Action
#Transformations - Result of transformations is new RDD.RDD is not human readable.

#To display or print  RDD use Action operations.

#Generate random data
import random
randomlist=random.sample(range(0,40),10)
print (randomlist)

#Create RDD -- with 4 partitions
rdd1=sc.parallelize(randomlist,4)
#Use Action operation Collect() to display, view or printout RDD Data.(Recommended for small data size)
rdd1.collect()

#Data distribution in partitions.
#In PySpark, the glom function combines all elements in each partition of a Spark RDD (Resilient Distributed Dataset) into a list.
#Glom treats a partition as an array instead of a single row, which can speed up some operations but also increases memory usage.
#For example, if you create two partitions with the values and, rdd.glom().collect() returns a list of lists containing [,]. 
#Glom returns an RDD created by coalescing all elements within each partition into a list.
#Glom is useful when you want to represent RDD operations as matrix manipulations,
# such as multiplying each row by a given weight vector or multiplying with a whole partition at a time
print ("No. of partitions: ",rdd1.getNumPartitions())
print ("Distribution of partitions:",rdd1.glom().collect())
print ("The two partitions:",rdd1.glom().take(2))

#Print last partition
rdd1.glom().collect()[3]

# Collect(),take(), getNumPartitions() are all examples of 'Action' operations.
# Whereas parallelize() and glom() is 'Transformations' operation.
# All of the below 'Action' operations are running on Driver node . Hence for large operations involving huge datasets we need to use the executors
#for all 'transformation' operations and display results by 'Action' operations.
#count()
rdd1.count()

#first()
rdd1.first()

#top()
rdd1.top(2)

#distinct()--> is not human readable as distinct is a 'transformations' function. 
rdd1.distinct()

# Use collect() along with distinct() to display data.
 rdd1.distinct().collect()
 
 def myfunc(item):
  return(item + 1) * 3
  
#map()
rdd_map=rdd1.map(myfunc)
rdd_map.collect()

rdd_map=rdd1.map(lambda item:(item + 1) * 3)
rdd_map.glom().collect()

#filter()   
rdd_filter=rdd1.filter(lambda x: x%3==0)
rdd_filter.collect()

# Filter is going to find conditional statements. It will avoid calculation if there is no conditional statement.
# Only map will do calculations upon the elemnts in of the RDD partition.

# Partitions which are not required are removed by RDD . We can check from below query.
rdd_filter.glom().collect()

#flatmap()
# Flattens out the partitions and elements are displayed in a single list . rdd1.flatMap(lambda x: [x + 2, x + 5]) & rdd_flatmap.collect()
rdd_flatmap=rdd1.flatMap(lambda x: [x + 2, x + 5])
rdd_flatmap.collect()
rdd_flatmap.glom().collect()

#flatmap() aggregations -> sum all the elements in each RDD partition. Then the sum results produced out of each partition will again be summed up.
# This will continue until the total summed up value is produced.
rdd_flatmap.reduce(lambda x,y: x + y)

#Descriptive Statistics
print ("Maximum value", rdd1.max())
print ("Minimum value", rdd1.min())
print ("Average value", rdd1.mean())
print ("Total value", rdd1.sum())
print ("Standard deviation value", round(rdd1.stdev(),2))

#mapPartitions()
#In Apache Spark, mapPartitions is a transformation that allows programmers to process partitions of data as a whole.
# It's the only narrow transformation that provides partition-wise processing.
#mapPartitions applies a function to each partition of an RDD, and executes the function once for each partition.
#mapPartitions is useful when you want to extract condensed information from each partition, such as finding the minimum and maximum of numbers. 
#It's also useful when the processing of each partition requires some initialization or setup that can be done once for each partition.
#MapPartitions is similar to map, but mapPartitions runs separately on each partition, while map processes partitions record-wise.
#The mapPartitions function in pyspark returns a new RDD by applying a function to each partition of the RDD. 

def myfunc(partition):
  sum=0
  for item in partition:
    sum=sum + item
  yield sum

rdd1.mapPartitions(myfunc).collect()

# max value from each partition.
def max_val_part(partition):
  max_val=0
  for item in partition:
    if item > max_val:
      max_val=item
  yield max_val

max_part_rdd=rdd1.mapPartitions(max_val_part)
max_part_rdd.collect()


#In PySpark, map() and mapPartitions() are two transformation operations that are used to apply a function to each element of an RDD in a distributed manner.
# However, there are some differences as follows:
#1. map()is a transformation operation that applies the specified function to each element of the RDD and returns a new RDD.

#The function passed to map() is applied individually to each element of the RDD.

#It operates on one element at a time and can be slower when the function has high overhead or requires external resources.

#2.mapPartitions()is a transformation operation that applies the specified function to each partition of the RDD and returns a new RDD.

#The function passed to mapPartitions() is applied to each partition as a whole, instead of individual elements.

#It can be more efficient when the function has a high overhead or requires external resources, 
#as it reduces the overhead of function invocation by processing multiple elements at once.


# First of all, make sure to load and initiate sc (SparkContext).
 #Here's a code example to illustrate the difference (firstly load and initiate sc instance):
# Create RDD:
rdd1 = sc.parallelize([7, 30, 36, 29, 20, 18, 9, 2, 23, 38], 4)
rdd1.collect()

# Define a function to double each element using map()
def double(x):
    return x * 2
    
 # Apply map() transformation
map_result_rdd = rdd1.map(double)

# Define a function to double each element using mapPartitions()
def double_partition(partition):
    sum = 0
    for item in partition:
      sum = sum + (item * 2)
    yield sum
    
  # Apply mapPartitions() transformation
mappartitions_result_rdd = rdd1.mapPartitions(double_partition)

# Print the results
print("RDD with partitions: ", rdd1.glom().collect())
print("Map Result: ", map_result_rdd.glom().collect())
print("MapPartitions Result: ", mappartitions_result_rdd.glom().collect())

#The results:

#RDD with partitions: [[7, 30], [36, 29], [20, 18], [9, 2, 23, 38]]

#Map Result: [[14, 60], [72, 58], [40, 36], [18, 4, 46, 76]]

#MapPartitions Result: [[74], [130], [76], [144]]

# Advanced RDD Transformations and Actions.
print (rdd1.collect())
rdd2=sc.parallelize([1,14,20,20,28,10,13,3],2)
print (rdd2.collect())

rdd_union = rdd2.union(rdd3).distinct()
rdd_union.collect()
# RDD1 with 4 partitions and RDD2 with 2 partitions . Total 6 partitions
print (rdd_union.getNumPartitions())

#Intersection()
rdd_intersection=rdd1.intersection(rdd2)
print (rdd1.intersection(rdd2).collect())
print(rdd1.intersection(rdd2).getNumPartitions())

# To find out empty partition. The below code will collapse if we use collect() against huge amount of data.
rdd_intersection.glom().collect()

#Find empty partitions

counter=0
for item in rdd_intersection.glom().collect():
  if len(item)==0:
    counter = counter + 1
print (counter)

#coalesce(numPartitions) -- To reduce the number of partitions
rdd_intersection.coalesce(1).glom().collect()

#COLLECT IS NOT RECOMMENDED FOR HUGE SIZE DATA. USE TAKE() .

#takesample (withReplacement,num,[seed])
# DO NOT RUN TAKESAMPLE() WITH LARGE DATASETS AS IT IS GOING TO RUN ON DRIVER NODE AND MAY NOT TAKE ADVANTAGE OF THE EXECUTOR PARALLELIZATION.
rdd1.takeSample(False,5)

# DO NOT USE WITH HUGE DATASETS AS IT MAY SLOW DOWN OPERATIONS AND MAY LEAD TO CRASH.
m#takeOrdered(n,[ordering])
print (rdd1.takeOrdered(5))
print (rdd1.takeOrdered(5, key=lambda x: -x))

#reduce()
#The Python reduce() function is used to apply a given function to a sequence of elements from left to right 
#and reduces it to a single value. This type of operation is commonly referred to as a “reduction” or “fold”.
# Remember, the reduce() function in Python is not a built-in function, but rather a part of the functools module.
rdd1.reduce (lambda x,y: x * y)
#Can be any aggregator .
#Eg: rdd1.reduce (lambda x,y: x - y)

#reduceByKey()
#In Apache Spark, the reduceByKey() function merges values for each key using a reduce function: 
#How it works
#Groups data by key, then applies a reduction function to the values associated with each key. 
#The result is an RDD with unique keys and a reduced value for each key.
#When to use it
#Useful when you want to perform an aggregate function on the values associated with each key.
#Merges values locally on each mapper before sending results to a reducer. Combiners (optimized mini-reducers) 
#are used in all cluster nodes before merging the values per partition.
#Partitioned with numPartitions partitions, or the default parallelism level if numPartitions is not specified.
# The default partitioner is hash-partition.

rdd_rbk=sc.parallelize([(1,4),(7,10),(5,7),(1,12),(7,12),(7,1),(9,1),(7,4)],2)
print (rdd_rbk.glom().collect())
print (rdd_rbk.reduceByKey(lambda x,y: x + y).collect())

#User friendly visualization.
import pandas as pd
Counter = pd.DataFrame({'key': rdd_rbk.keys().collect(),
                        'values': rdd_rbk.values().collect()})
Counter

#sortByKey()
rdd_rbk.reduceByKey(lambda x,y: x + y).sortByKey(True).collect()

#countByKey()
rdd_rbk.countByKey()

# Can also use sorted() Python method.
sorted(rdd_rbk.countByKey().items())

#Difference between  reducebykey and groupbykey

#In Apache Spark, the main difference between reduceByKey and groupByKey is how they combine values associated with each key in a key-value RDD:
#groupByKey
#Groups values and returns an iterable collection. It's used when you need to group data by key and process all the values associated with each key together.
# For example, you might use groupByKey for word count or grouping data for further analysis.
# However, groupByKey can be inefficient for large datasets because it involves shuffling all the data.

#reduceByKey
#Applies a reduction function and returns a single value for each key. It's used when you need to perform aggregations or computations
 #on grouped values based on their keys. ReduceByKey is generally more efficient than groupByKey because
 #it performs a local reduction of values on each partition before shuffling.
 
 #Here are some other differences between reduceByKey and groupByKey:
#Data transfer
#ReduceByKey doesn't cause unnecessary data transfer over the network, while groupByKey does.
#Combiner
#ReduceByKey uses an implicit combiner, while groupByKey doesn't use a combiner.
#Parameters
#ReduceByKey takes two parameters, one for the SeqOp and the other for the CombOp, while groupByKey doesn't use any parameters as functions.

##groupByKey()

rdd_group=rdd_rbk.groupByKey()
rdd_group.getNumPartitions()

rdd_group.collect()

for item in rdd_group.collect():
  print (item[0],[values for values in item[1]])

#lookupByKey()
rdd_rbk.lookup(7)

#cache
# By default, each transformed RDD may be recomputed each time you run an action on it.
# However you may also persist an RDD in memory using the persist (or cache) method.
# In which case Spark will keep the elements around on the cluster for much faster access the next time you query it.
# It will not be deleted for garbage collection or any removal policy of Spark.

#cache
rdd_rbk.persist()

#Persistence (https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence)
# Data gets distributed between memory and disk.

from pyspark import StorageLevel
rdd1.persist(StorageLevel.MEMORY_AND_DISK)

MEMORY_AND_DISK_2, it says RDD partitions will have replication of 2.

#===============================================================================================================================#

# PySpark with DF or DataFrames or Relational Databases.
#JSON , CSV , Hive , Parquet

#RDD - > SparkContext , 
#DF -> SparkSession

#DF Operations

#DF Standalone (operation - transformation & action) 
#transformation -> SELECT, WHERE (FILTER) , GROUPBY , HAVING , JOIN 

#action -> SHOW()

# SPark SQL & UDF (User defined functions)

#Optimization technique -> Catalyst Query Optimizer & Project Tungsten to optimize CPU and memory optimization.

# A) Catalyst Query Optimizer -> Rule based & Cost based optimization techniques.
# B) Project Tungsten -> Memory management , Cache awareness , Code generation (modern compilers for parallel processing.)

# CREATE DF AND BASIC OPERATIONS.
df1=spark.read.format("csv").load("/content/sample_data/CompleteDataset.csv", inferSchema=True, header=True)

#Show data:
df1.show()

# How many partitions in DF. By default 2 partitions.
df1.rdd.getNumPartitions()

# Repartition -  Increase/ Decrease the partitions in Df
df2=df1.repartition(4)
df2.rdd.getNumPartitions()

# To reduce number of partitions
df3=df2.coalesce(1)

df2.show()

#Rename columns and amend NULLs
df2=df2.withColumnRenamed("_c0","ID")\
    .withColumnRenamed("Ball control","Ball_Control")\
    .withColumnRenamed("Sliding tackle", "Sliding_Tackle")

df2.na.fill({"RAM":10,"RB":1}).show()

#df2.dropna()

#Transformation (SELECT)
df2.select("Name","Overall").distinct().show()

# Transformation(FILTER)
df2.filter(df2["Overall"] > 70).show()

# Transformation(FILTER)
df2.select("Overall","Name","Age").filter(df2["Overall"] > 70).show()

#We can also use WHERE.
df2.select("Overall","Name","Age").where(df2["Overall"] > 70).show()

# Transformation(FILTER)

# Transformation(FILTER)
df4.select("Overall","Name","Age").where(col("Overall")>70).orderBy("Overall").show()

#Visualize the results
df2_result=df2.where(df/2["Overall"] > 70).groupBy("Age").count().sort("Age")
pandas_df=df2_result.toPandas()
pandas_df.plot(x="Age",y="count", kind="bar")
    
pandas_df.sort_values(by="count", ascending=False).plot(x="Age",y="count", kind="bar")

#ADVANCED DF OPERATIONS - SPARK SQL AND UDF
#Spark SQL (Register Df with a local temporary View)

df2.createOrReplaceTempView("df_football")

#SQL query
sql_query="""SELECT Age, count(*) as Count 
                    FROM df_football 
                    WHERE Overall > 70 
                    GROUP BY Age 
                    ORDER BY Age"""
result=spark.sql(sql_query)
result.show()

#UDF - User Defined Functions
def uppercase_converter(record):
  if len(record) > 10:
    return record.upper()
  else:
    return record.lower()

# register the DF
df2.createOrReplaceTempView("UDF_football")

# register the function
spark.udf.register("UPPER", uppercase_converter)

# Use the UDF in SQL
sql_query= "SELECT Age, UPPER(Name) as Name, UPPER(club) as Club FROM UDF_football"

result= spark.sql(sql_query)
result.show()

df2.select ("RowID","Order_ID","Order_Date","Ship_Date","Ship_Mode","Customer_ID","Customer_Name","Address_Country","Address_City","Address_State")\
.where((col('Ship_Mode')=='First Class') & (col('Address_State')=='Texas')).show()

#================== PYSPARK FOR LARGE STRUCTURED DATA====================================================================

# ONLY IN UBUNTU MACHINE
!pip3 install -q findspark
import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
# INITIALIZING SPARK
conf=SparkConf().setAppName("KDDCup_PySpark").setMaster("local[*]")
sc=SparkContext(conf=conf)
print (sc)
print ("Ready to go!")

#==========================================================================================================================
#DATAFRAME CREATION

#You can manually create a PySpark DataFrame using toDF() and createDataFrame() methods, 
#both these function takes different signatures in order to create DataFrame from existing RDD, list, and DataFrame.
#
#You can also create PySpark DataFrame from data sources like TXT, CSV, JSON, ORV, Avro, Parquet, XML formats 
#by reading from HDFS, S3, DBFS, Azure Blob file systems e.t.c.


#In order to create a DataFrame from a list we need the data hence, first, let’s create the data and the columns that are needed.

columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

#1. Create DataFrame from RDD

#One easy way to manually create PySpark DataFrame is from an existing RDD. first, let’s create a Spark RDD from a collection List by calling parallelize() function from SparkContext .
# We would need this rdd object for all our examples below.

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
rdd = spark.sparkContext.parallelize(data)

#PySpark RDD’s toDF() method is used to create a DataFrame from the existing RDD. Since RDD doesn’t have columns,
# the DataFrame is created with default column names “_1” and “_2” as we have two columns.

dfFromRDD1 = rdd.toDF()
dfFromRDD1.printSchema()

#PySpark printschema() yields the schema of the DataFrame to console.

#If you wanted to provide column names to the DataFrame use toDF() method with column names as arguments as shown below.

columns = ["language","users_count"]
dfFromRDD1 = rdd.toDF(columns)
dfFromRDD1.printSchema()

#This yields the schema of the DataFrame with column names. use the show() method on PySpark DataFrame to show the DataFrame
#Using createDataFrame() from SparkSession
#Using createDataFrame() from SparkSession is another way to create manually and it takes rdd object as an argument.
# and chain with toDF() to specify name to the columns.

dfFromRDD2 = spark.createDataFrame(rdd).toDF(*columns)

#Create DataFrame from List Collection
#In this section, we will see how to create PySpark DataFrame from a list. 
#These examples would be similar to what we have seen in the above section with RDD, 
#but we use the list data object instead of “rdd” object to create DataFrame.

#Using createDataFrame() from SparkSession
#Calling createDataFrame() from SparkSession is another way to create PySpark DataFrame manually,
# it takes a list object as an argument. and chain with toDF() to specify names to the columns.

dfFromData2 = spark.createDataFrame(data).toDF(*columns)

#Using createDataFrame() with the Row type
#createDataFrame() has another signature in PySpark which takes the collection of Row type and schema 
#for column names as arguments. To use this first we need to convert our “data” object from the list to list of Row.

rowData = map(lambda x: Row(*x), data) 
dfFromData3 = spark.createDataFrame(rowData,columns)

#Create DataFrame with schema
#If you wanted to specify the column names along with their data types,
# you should create the StructType schema first and then assign this while creating a DataFrame.

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
data2 = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
  ])
 
df = spark.createDataFrame(data=data2,schema=schema)
df.printSchema()
df.show(truncate=False)

#CREATE DATAFRAME FROM DATA SOURCES
#In real-time mostly you create DataFrame from data source files like CSV, Text, JSON, XML e.t.c.

#PySpark by default supports many data formats out of the box without importing any libraries and to create DataFrame 
#you need to use the appropriate method available in DataFrameReader class.

#From CSV:
#Use csv() method of the DataFrameReader object to create a DataFrame from CSV file. you can also provide options like what delimiter to use,
# whether you have quoted data, date formats, infer schema, and many more

df2 = spark.read.csv("/src/resources/file.csv", header=True)
df2.show(50)

#From TXT:
#Similarly you can also create a DataFrame by reading a from Text file, use text() method of the DataFrameReader to do so.
#In pyspark to show the full contents of the columns, you need to specify truncate=False to show() method. for eg show(truncate=False)

df2 = spark.read.text("/src/resources/file.txt")
text_df.show(50,truncate=False)

#From JSON:
#PySpark is also used to process semi-structured data files like JSON format.
 #you can use json() method of the DataFrameReader to read JSON file into DataFrame. Below is a simple example.
 
 df2 = spark.read.json("/src/resources/file.json")
 
 #Similarly, we can create DataFrame in PySpark from most of the relational databases,by reading Avro, Parquet, ORC, Binary files and accessing Hive and HBase table,
 #and also reading data from Kafka



#A PySpark DataFrame can be created via pyspark.sql.SparkSession.createDataFrame typically by passing a list of lists,
#tuples, dictionaries and pyspark.sql.Rows, a pandas DataFrame and an RDD consisting of such a list. pyspark.sql.SparkSession.createDataFrame takes the schema argument
#to specify the schema of the DataFrame. When it is omitted, PySpark infers the corresponding schema by taking a sample from the data.
#Firstly, you can create a PySpark DataFrame from a list of rows.

from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row

df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
])
df.show()

#Create a PySpark DataFrame with an explicit schema.

df = spark.createDataFrame([
    (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
    (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
], schema='a long, b double, c string, d date, e timestamp')    
df.show()

#Create a PySpark DataFrame from a pandas DataFrame.
pandas_df = pd.DataFrame({
    'a': [1, 2, 3],
    'b': [2., 3., 4.],
    'c': ['string1', 'string2', 'string3'],
    'd': [date(2000, 1, 1), date(2000, 2, 1), date(2000, 3, 1)],
    'e': [datetime(2000, 1, 1, 12, 0), datetime(2000, 1, 2, 12, 0), datetime(2000, 1, 3, 12, 0)]
})
df = spark.createDataFrame(pandas_df)
df.show()

#pyspark.sql.DataFrame.withColumn --> Returns a new DataFrame by adding a column or replacing the existing column that has the same name.
#The column expression must be an expression over this DataFrame; attempting to add a column from some other DataFrame will raise an error.

#This method introduces a projection internally. Therefore, calling it multiple times, for instance, via loops in 
#order to add multiple columns can generate big plans which can cause performance issues and even StackOverflowException.
# To avoid this, use select() with multiple columns at once.

df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], schema=["age", "name"])
df.withColumn('age2', df.age + 2).show()

+---+-----+----+
|age| name|age2|
+---+-----+----+
|  2|Alice|   4|
|  5|  Bob|   7|
+---+-----+----+

#============================================================================================================================

#Spark Data structures - RDD , DataFrames , Datasets and differences.

#Spark RDD, DataFrame, and Dataset are three important abstractions in Apache Spark that allow developers to work with structured data 
#in a distributed computing environment. While they all provide a way to represent data, they differ in several ways. 
#RDD is a low-level API with more control over data, while DataFrame and Dataset are high-level APIs optimized for performance. 
#DataFrames are not type-safe, while Datasets provide compile-time type checking.
 #Additionally, Datasets are faster than DataFrames as they use JVM bytecode generation for operations on data.
 
 #One can easily convert between Spark RDD, DataFrame, and Dataset using built-in conversion methods. 
 #RDDs can be converted to DataFrames and vice versa using the toDF() and rdd() methods, 
 #respectively. Datasets can be created from RDDs using the createDataset() method and can be converted to DataFrames using the toDF() method.
 
 #RDDs provide full control over memory management and can be cached in memory or on disk.
 #DataFrames have optimized memory management with a Spark SQL optimizer that helps reduce memory usage.
 #Datasets also have optimized memory management and support most of the available data types.
 
 #Datasets are faster than DataFrames and RDDs as they use JVM bytecode generation for operations on data.
 

#===========================================================================================================================
#The list of joins provided by Spark SQL is:

#1)Inner Join
#2)Left / Left Outer Join
#3)Right / Right Outer Join
#4)Outer / Full Join
#5)Cross Join
#6)Left Anti Join
#7)Left Semi Join
#8)Self Join

#1)Inner Join
#Returns only the rows from both the dataframes that have matching values in both columns specified as the join keys.

df1.join(df2, df1['key'] == df2['key'], 'inner').show()

#2)Left / Left Outer Join
#Returns all the rows from the left dataframe and the matching rows from the right dataframe.
# If there are no matching values in the right dataframe, then it returns a null.

df1.join(df2, df1['key'] == df2['key'], 'left').show()
(OR)
df1.join(df2, df1['key'] == df2['key'], 'leftouter').show()

#3)Right / Right Outer Join
#Returns all the rows from the right dataframe and the matching rows from the left dataframe.
# If there are no matching values in the left dataframe, then it returns a null.

df1.join(df2, df1['key'] == df2['key'], 'right').show()
(OR)
df1.join(df2, df1['key'] == df2['key'], 'rightouter').show()

#4)Outer / Full Join
#Returns all the rows from both the dataframes, including the matching and non-matching rows.
 #If there are no matching values, then the result will contain a NULL value in place of the missing data.
 
 df1.join(df2, df1['key'] == df2['key'], 'outer').show()
(OR)
df1.join(df2, df1['key'] == df2['key'], 'full').show()
(OR)
df1.join(df2, df1['key'] == df2['key'], 'fullouter').show()
 
 #5) Cross join
 #Returns all possible combinations of rows from both the dataframes. In other words, it takes every row from one dataframe 
 #and matches it with every row in the other dataframe. 
 #The result is a new dataframe with all possible combinations of the rows from the two input dataframes.
 
 df1.crossJoin(df2).show()
 
 #6) Left Anti Join
#A left anti join in Spark SQL is a type of left join operation that returns only the rows from the left dataframe 
#that do not have matching values in the right dataframe.
 #It is used to find the rows in one dataframe that do not have corresponding values in another dataframe.
 
 #The result of a left anti join is a dataframe that contains only the rows from the left dataframe that do not have matching values in the right dataframe. 
 #If a row from the left dataframe has matching values in the right dataframe, it will not be included in the result.
 
 df1.join(df2, df1['key'] == df2['key'], 'left_anti').show()
 
 #7) Left Semi Join
#A left semi join in Spark SQL is a type of join operation that returns only the columns from the left dataframe
# that have matching values in the right dataframe. It is used to find the values in one dataframe that have corresponding values 
#in another dataframe.

#The result of a left semi join is a dataframe that contains only the columns from the left dataframe
 #that have matching values in the right dataframe. The columns from the right dataframe are not included in the result.
 
 df1.join(df2, df1['key'] == df2['key'], 'leftsemi').show()
 
 #8) 
 #Self Join
#A self join in Spark SQL is a join operation in which a dataframe is joined with itself.
# It is used to compare the values within a single dataframe and return the rows that match specified criteria.

#For example, a self join could be used to find all pairs of rows in a dataframe where the values in two columns are equal.
# The result would be a new dataframe that contains only the rows that meet the specified criteria.

df.alias("df1").join(df.alias("df2"), df1['key'] == df2['key']).show()

#==============================================================================================================================
from google.colab import drive
drive.mount('/content/drive')

# Read and Load Data to Spark
# Data source : http://kdd.ics.uci.edu/databases/kddcup99/kddcup99.html

rdd=sc.textFile("/content/drive/MyDrive/kddcup.data.gz")

# Repartition and cache data :
# Repartition and cache data :
rdd=rdd.repartition(10) # shuffle all data
print (sc.defaultParallelism)
print (rdd.getNumPartitions())

rdd.persist()
#There are 2 cores and 5 partitions in each core.

#COUNT ELEMENTS
rdd.count()

# Get 10 records randomly
rdd.takeSample(False, 10 , 1234)

# RDD counts data line by line whereas dataframe counts by columns.

# Calculate the ratio of normal connections.

# Calculate the ratio of 'normal' connections
Normal_rdd=rdd.filter(lambda line: 'normal.' in line)

ratio= Normal_rdd.count() / rdd.count()

print ("the ratio of normal connections is {} %".format(round(ratio,4)* 100))

# Get the list of labels
Split_rdd=rdd.map(lambda line: line.split(','))
Label_rdd= Split_rdd.map(lambda item: item[-1]).distinct()
Label_rdd.collect()

# Count the number of connections (records) for each label
def Labelcount_func(items):
  Labels_count=[]
  for i in items:
    Labels_count.append(rdd.filter(lambda line: i in line).count())
    

# Time consuming

%%time
Labelcount_func(Label_rdd.collect())

# 2nd solution: create <key , value> pairs
%%time
Label_rdd_KV=Split_rdd.map(lambda x:(x[-1], 1))
Label_rdd_Reduce= Label_rdd_KV.reduceByKey(lambda a,b : a + b)

import pandas as pd

Keys=Label_rdd_Reduce.keys().collect()
Values=Label_rdd_Reduce.values().collect()

DF_labels_KV=pd.DataFrame({'Label': Keys,
                           'Count': Values})
DF_labels_KV.sort_values(by='Count',ascending=False)







Applying Schemas
=====================

Inferring Schema
----------------------------------------------------------
Great for development or when incoming files are dynamic.
Challenges:
-Scanning big datasets will take a lot of time.
- Corrupt files will cause failures , difficult to debug.

Recommended to manually define schema for DataFrames in production.
Example:
yellowTaxiSchema= (
                     StructType
                     (
                     [
                     ]
                     )
                     

# Required for StructField, StringType, IntegerType, etc.
from pyspark.sql.types import *

csvSchema = StructType([
  StructField("timestamp", StringType(), False),
  StructField("site", StringType(), False),
  StructField("requests", IntegerType(), False)
])

#Read in our data (and print the schema).

#We can specify the schema, or rather the StructType, with the schema(..) command:

(spark.read                   # The DataFrameReader
  .option('header', 'true')   # Ignore line #1 - it's a header
  .option('sep', "\t")        # Use tab delimiter (default is comma-separator)
  .schema(csvSchema)          # Use the specified schema
  .csv(csvFile)               # Creates a DataFrame from CSV after reading in the file
  .printSchema()
)

# Required for StructField, StringType, IntegerType, etc.
from pyspark.sql.types import *

jsonSchema = StructType([
  StructField("channel", StringType(), True),
  StructField("comment", StringType(), True),
  StructField("delta", IntegerType(), True),
  StructField("flag", StringType(), True),
  StructField("geocoding", StructType([
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("countryCode2", StringType(), True),
    StructField("countryCode3", StringType(), True),
    StructField("stateProvince", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True)
  ]), True),
  StructField("isAnonymous", BooleanType(), True),
  StructField("isNewPage", BooleanType(), True),
  StructField("isRobot", BooleanType(), True),
  StructField("isUnpatrolled", BooleanType(), True),
  StructField("namespace", StringType(), True),
  StructField("page", StringType(), True),
  StructField("pageURL", StringType(), True),
  StructField("timestamp", StringType(), True),
  StructField("url", StringType(), True),
  StructField("user", StringType(), True),
  StructField("userURL", StringType(), True),
  StructField("wikipediaURL", StringType(), True),
  StructField("wikipedia", StringType(), True)
])
#Just like CSV, providing the schema avoids the extra jobs.
#The schema allows us to rename columns and specify alternate data types.
#Can get arbitrarily complex in its structure.

#Let's take a look at some of the other details of the DataFrame we just created for comparison sake.

jsonDF = (spark.read
  .schema(jsonSchema)
  .json(jsonFile)    
)
print("Partitions: " + str(jsonDF.rdd.getNumPartitions()))
printRecordsPerPartition(jsonDF)
print("-"*80)

#And of course we can view that data here:
display(jsonDF)

#That was a lot of typing to get our schema!
#For a small file, manually creating the the schema may not be worth the effort.
#However, for a large file, the time to manually create the schema may be worth the trade off of a really long infer-schema process.

# Saving Processed Data to Files
-----------------------------------------

# Reduce number of DataFrame partitions to 4.

yellowTaxiDF= yellowTaxiDF.coalesce(4)
yellowTaxiDF.rdd.getNumPartitions() 

# Save data in CSV format to storage.
(
    yellowTaxiDF
           .write 
           .option ("header", "true")
           .option ("dateformat", "yyyy-MM-dd HH:mm:ss.S")
           .mode("overwrite") # Options - Append , ErrorIfExists , Ignore , Overwrite
           .csv ("C:\SparkCourse\DataFiles\Output\YellowTaxisOutput.csv")

)

Append - Append DataFrame content to existing location , or create new one.
ErrorIfExists - Throw error if files already exist.
Ignore - If files already exist , don’t do anything.
Overwrite - Remove existing files and add new ones.

Apache Parquet -> 
-Columnar storage format.
-Support complex nested data structures.
-Stores schema in the file itself.
-Supports efficient compression & encoding.
- Binary files.
- Querying is much faster than CSV / JSON .

# Save data in Parquet format to storage.
(
    yellowTaxiDF
           .write 
           .option ("header", "true")
           .option ("dateformat", "yyyy-MM-dd HH:mm:ss.S")
           .mode("overwrite") # Options - Append , ErrorIfExists , Ignore , Overwrite
           .csv ("C:\SparkCourse\DataFiles\Output\YellowTaxisOutput.parquet")

)

# Running SQL queries on DataFrames
---------------------------------------------

To access DataFrames with SQL use temp View.

df1.createOrReplaceTempView("view1")

Create SQL View on top of DataFrame
- Think of this as a pointer to the DataFrame.

Run SQL queries on th View.

Same performance in SQL as using DataFrames in other languages due to Catalyst Optimizer.

Use spark.sql in PySpark / Scala to run SQL queries.

spark.sql returns output of query as DataFrame.

# Create a sQL View based on Python DataFrame.It's a in-memory , temporary view and will remain active till Spark session is alive.

yellowTaxiDF.createOrReplaceTempView("YellowTaxis")

# Run SQL query on View

outputDF= spark.sql("SELECT * FROM  YellowTaxis WHERE PULocationID=171" )

outputDF.show()

# Read Green Taxis TSV file.

greenTaxiDF= (
                 spark
                 .read
                 .option("header", "true")
                 .option("delimiter", "\t")
                 .csv("C:\SparkCourse\DataFiles\Raw\GreenTaxis_202210.csv")
                 
greenTaxiDF.createOrReplaceTempView("GreenTaxis")

# Write SQL query to merge Yellow and Green Taxis data.

spark.sql ("""

SELECT 'Yellow'                              AS TaxiType
       , lpep_pickup_datetime                AS PickupTime
       , lpep_dropoff_datetime                AS DropTime
       , PULocationID                        AS PickupLocationId
       , DOLocationID                        AS DropLocationId
FROM YellowTaxis

UNION ALL 

SELECT 'Green'                           AS TaxiType

     ,lpep_pickup_datetime               AS PickupTime
     , lpep_dropoff_datetime             AS DropTime
     , PULocationID                     AS   PickupLocationId
     , DOLocationID                     AS  DropLocationId
     
FROM GreenTaxis

""").show()

# Read Taxi Zones data , and create Global Temp View.

taxiZonesSchema= "LocationID INT, Borough STRING, Zone STRING, ServiceZone STRING"

taxiZonesDF= (
                 spark
                 .read
                 .schema(taxiZonesSchema)
                 .csv("D:\DemoFiles\SparkCourseFiles\TaxiZones.csv")
            )
            
# Create Global, temp View.

taxiZonesDF.createOrReplaceGlobalTempView("TaxiZones")

taxiZonesDF.show()

# Temp View - Calid for a Spark session.
# Global Temp View - Valid for a Spark application.

# Create a report - figure out number of rides , grouped by Borough and type of Taxi.

spark.sql("""

SELECT Borough, TaxiType, COUNT(*) AS TotalTrips

FROM  global_temp.TaxiZones

LEFT JOIN
(
SELECT  'Yellow' AS TaxiType, PULocationID FROM YellowTaxis

UNION ALL

SELECT 'Green' AS TaxiType , PULocationID FROM GreenTaxis
) AllTaxis

ON AllTaxis.PULocationID=TaxiZones.LocationID

GROUP BY Borough, TaxiType
ORDER BY Borough, TaxiType

""").show()

#The "global_temp" namespace is used to access global temporary views.
- spark.sql("SELECT * FROM global_temp.people").show()
#You can now perform SQL queries and other data analysis tasks on the global temporary view, regardless of which Spark session you are in.


# Working with Spark Tables
------------------------------------------

Writing Data from DataFrame.


Directly to Storage                                                        |        As Spark Table
- Files stored in defined format in Storage.                               |    - Files stored in defined format in Storage.
- No metadata registered                                                   |    - Metadata registered with a metastore or catalog. 
- To query , read files from Storage.                                      |    - To query either read files from the Storage or directly reference the table.


DataFrame -> Df1

df1.write.saveAsTable ("mytable")

SQL -> SELECT * FROM mytable

PySpark -> df1.read.table("mytable")

# This will query the catalog which will read data from Storage and return the output in tabular format.
# Helps to manage datasets.
# No need to define or infer schema while reading since it is defined in the Catalog.

# Catalog Types ->
# -  a) In-memory Catalog - Only works in a session. Cleaned up when session ends.
# - b) Persistent Catalog - Metadata is permanently stored. Spark has built-in Hive Catalog.

# Table Types ->
# a) Managed Table 
1. Schema and data is managed by Spark.
2. Data is stored in default location.
3. Dropping table deletes both Schema and Data.
4. Useful to persist staging data.

# b) Unmanaged / Exernal Table.
1. Only Schema is managed by Spark.
2. Data is stored in an external location.
3. Dropping table deletes only schema not data.
4. Useful to persist processed data.

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=(
           SparkSession
               .builder
               .appName("SparkTablesApp")
               .master("local[4]")
               
               .config("spark.dynamicAllocation.enabled" , "false")
               .config("spark.sql.adaptive.enabled", "false")
               
               #Enable Hive support. If we do not enable the session will be in-memory Catalog.
               .enableHiveSupport()
               
               .getOrCreate()
               
   )
   
 sc= spark.sparkContext

spark

# Create schema for Yellow Taxi Data.

# Read the data into a DataFrame and infer schema manually with the above created schema.

# To check existing databases OR register new Databases in Hive Metastore.

spark.sql("""

SHOW DATABASES

""").show()

spark.sql ("""
CREATE DATABASE IF NOT EXISTS TaxisDB
""")

# Save DataFrame as a Managed Spark Table in Hive.

(
yellowTaxiDF
         .write
         .mode ("overwrite")
         .saveAsTable("TaxisDB.YellowTaxisManaged")
)

spark.sql("""
SHOW TABLES IN TaxisDB
""").show(50,truncate=False)

# Run queries on Managed Spark Table.

spark.sql("""
SELECT *
FROM TaxisDB.YellowTaxisManaged
LIMIT 10
""").show()

outputDF = (
              spark
              .read
              .table("TaxisDB.YellowTaxisManaged")
          )
          
outputDF.limit(10).show()

spark.sql("""
DESCRIBE TABLE EXTENDED TaxisDB.YellowTaxisManaged
""").show(50, truncate=False) 

# Save DataFrame as an Unmanaged / External Spark Table in Hive.

(
    yellowTaxiDF
               .write 
               .mode("overwrite")
               .option("path", "C:\SparkCourse\DataFiles\Output\YellowTaxisOutput.parquet")
               
              #.option("format", "csv")                     #Default is 'Parquet'
              
               .saveAsTable("TaxisDB.YellowTaxis")
 )
 
 # Drop External table and recreate using stored files.
 
 spark.sql("""
 DROP TABLE TaxisDB.YellowTaxis
 """)
 
 # Check the underlying files. Those have not been removed.
 
 # Re-Create the table from the underlying stored files.
spark.sql ("""
CREATE TABLE TaxisDB.YellowTaxis

USING PARQUET

LOCATION "C:/SparkCourse/DataFiles/Output/YellowTaxisOutput.parquet/" 

""")

# Working with User Defined Functions (UDFs)
-Invoked for each row in DataFrame.
- Register using udf() to use with DataFrames.
- Register using spark.udf.register() to use in SQL.

# Read Cabs file
cabsDF=(
               spark
               .read
               .option("header", "true")
               .option("inferSchema", "true")
               
               .csv("C:\SparkCourse\DataFiles\Raw\cabs.csv")
       )

# Create temp View
cabsDF.createOrReplaceTempView("Cabs")

cabsDF.show(truncate=False)       

# Create a function to convert case.

def ConvertCase(str):
   result= ""
   nameWordsArray=str.split(",")
   
   for nameWord in nameWordsArray:
     result=(result 
               + nameWord[0:1].upper()                           # Ex- for word 'MOHIT' returns => 'M'
               + nameWord[1:len(nameWord)].lower()               # Ex- for word 'MOHIT' returns => 'ohit'
               + ","
             )
             
    result= result [0:len(result) - 2]
    
    return result
    
# Use UDF in DataFrame code
(
    cabsDF
        .select(
                   "Name",
                   convertCaseUdf(col("Name")).alias("Name_ConvertedCase")
             )

            # .withColumn("Name_ConvertedCase", convertCaseUdf(col("Name")))

 ).show(truncate=False)            

# Register function as a User Defined Function (UDF)
- This registration option is for using UDF in SQL.

spark.udf.register("ConvertCaseSqlUdf" , convertCase, StringType())

# Use UDF in SQL query.

spark.sql("""
       SELECT Name 
           , convertCaseSqlUdf(Name) As Name_ConvertedCase
         FROM Cabs
 """).show(truncate=False)
 
 ** Note :- UDF does not allow Spark to do optimizations.
 
 # Performing operations on multiple Datasets.
 - Set & Join operations

# Display location information for each ride.
# Join DataFrames of YellowTaxis and Taxi Zones
joinedDF=(
            yellowTaxiDF
                 .join 
                 ( taxiZonesDF,
                 yellowTaxiDF.PickupLocationId==taxiZonesDF.PickupLocationId,
                 
                 "inner" 
               )
           )
           
joinedDF.printSchema()

# Join on same column name 
# Only one column shows up in output.


joinedDF=(
            yellowTaxiDF.alias("yt")
                 .join 
                 ( taxiZonesDF.alias("tz"),
                 #col["yt.PickupLocationId") == col("tz.PickupLocationId")
                 ['PickupLocationId'] ,# only one PickupLocationId column will be kept
                 
                 "inner" 
               )
           )
joinedDF.printSchema()

# SET Operations 
- Applies to two datasets having same schema , without conditions
- Output dataset has same schema as input datasets.
- Python / Scala / SQL 

# Spark supports
- Union, Union All, Intersect , Except / Minus

# Read Drivers file
driversDF=(
             spark
             .read
             .option("header","true")
             .option("inferSchema", "true")
             .csv("C:\SparkCourse\DataFiles\Raw\Drivers.csv")
             
             
        )
             
# Create temp view
driversDF.createOrReplaceTempView("Drivers")

driversDF.show()

# Read Cabs file
cabsDF=(
           spark
             .read
             .option("header","true")
             .option("inferSchema", "true")
             .csv("C:\SparkCourse\DataFiles\Raw\Cabs.csv")
         )      
         
# Create temp view
cabsDF.createOrReplaceTempView("Cabs")

cabsDF.show()

# Create list of all drivers

( spark.sql("""

       (
          SELECT Name
          FROM Cabs
          WHERE LicenseType = 'OWNER MUST DRIVE'
       )

         UNION ALL 
      (

        SELECT Name 
         FROM Drivers
     )
""")).count()

# Create list of unique drivers
( spark.sql("""

       (
          SELECT Name
          FROM Cabs
          WHERE LicenseType = 'OWNER MUST DRIVE'
       )

         UNION 
      (

        SELECT Name 
         FROM Drivers
     )
""")).count()

# Create list of all registered drivers who are driving cabs.

( spark.sql("""

       (
          SELECT Name
          FROM Cabs
          WHERE LicenseType = 'OWNER MUST DRIVE'
       )

         INTERSECT 
      (

        SELECT Name 
         FROM Drivers
     )
""")).count()


# Create list of drivers driving cabs , but not registered.
( spark.sql("""

       (
          SELECT Name
          FROM Cabs
          WHERE LicenseType = 'OWNER MUST DRIVE'
       )

         EXCEPT 
      (

        SELECT Name 
         FROM Drivers
     )
""")).count()

# Performing Window Operations
- Window is a subset of related rows in a DataFrame.
- Defines how rows are related , say by time or location.

Apply operations
- count , sum , average , min, max , etc.

- For each row in Window , operation is applied on all the rows of Window.

SELECT *
, Expense * 100 / TotalExpenses   AS ExpensePercent

FROM (
       SELECT Department , Expense
               ,SUM (Expense) OVER() As TotalExpenses
               
       FROM departments
    ) subquery
    
#   Window Partitions
Window can be further divided into Partitions
Operations are then applied on Partitions
 - count , sum , average , min, max , etc.

SELECT *
     , Salary * 100 / DeptPayout              As SharePercent
FROM (
SELECT Department, Employee , Salary
    ,SUM(Salary) OVER (PARTITION BY department) AS DeptPayout
FROM departments
)subquery

import findspark
findspark.init()
findspark.find()

from IPython.display import *
display (HTML("<style>pre { white-space: pre !important; } </style>"))


from pySpark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark= (
         SparkSession
         .builder
         .appName("WindowOperationsApp")
         .master("local[4]")

         .config("spark.dynamicAllocation.enabled", "false")
         .config("spark.sql.adaptive.enabled" , "false")
         
         .getOrCreate()
      )
      
 sc= spark.sparkContext
 
 # Read Yellow Taxis file.
 
 yellowTaxiDF= (
 
                   spark
                   .read 
                   .option ("header", "true")
                   .schema(yellowTaxiSchema)
                   .csv("D:\DemoFiles\SparkCourseFiles\YellowTaxis_202210.csv")
             )
             
 # Create temp view
 yellowTaxiDF.createOrReplaceTempView("YellowTaxis")
 
 # Create schema for Taxi Zones data
 taxiZonesSchema= "PickupLocationId INT, Borough STRING, Zone STRING, ServiceZone STRING"
 
 # Read Taxi Zones file
 taxiZonesDF= (
                  spark
                  .read 
                  .schema (taxiZonesSchema)
                  .csv("D:\DemoFiles\SparkCourseFiles\TaxiZones.csv")
             )
             
# Create temp view
taxiZonesDF.createOrReplaceTempView("TaxiZones")

# Windows - Find share of each borough in terms of rides.

taxiRidesDF = (
    spark.sql("""
                  SELECT tz.borough
                       , COUNT(*) AS RideCount
                       
                   FROM TaxiZones tz
                      INNER JOIN YellowTaxis yt ON yt.PickupLocationId = tz.PickupLocationId
                      
                   GROUP BY tz.Borough
                
            """)
     )

taxiRidesDF.createOrReplaceTempView("TaxiRides")

taxiRidesDF.orderBy("Borough").show()

# Calculate total rides across all boroughs
a) Create Window over entire table.
b) Add total rides (across all boroughs) against each row.

taxiRidesWindowDF = (
      spark.sql ("""
                      SELECT *
                      , SUM (RideCount) OVER() AS TotalRideCount
                      
                      FROM TaxiRides
               """)
         )

taxiRidesWindowDF.orderBy("Borough").show()

taxiRidesWindowDF.createOrReplaceTempView("TaxiRidesWindow")

# Find share of each borough in terms of rides.

#Divide Borough's ride count with Total Ride Count (across all Boroughs)

( spark.sql ("""
               SELECT *
               , ROUND ( (RideCount * 100) / TotalRideCount , 2) AS RidesSharePercent
               
               FROM TaxiRidesWindow
               ORDER BY Borough
        """)).show()
               
# Window Partitions - Find share of each zone in terms of rides , within their borough

#1. Get rides for each zone 
- Zone is part of borough


taxiRidesDF = (
    spark.sql("""
                  SELECT tz.borough
                      , tz.Zone
                       , COUNT(*) AS RideCount
                       
                   FROM TaxiZones tz
                      INNER JOIN YellowTaxis yt ON yt.PickupLocationId = tz.PickupLocationId
                      
                   GROUP BY tz.Borough
                            , tz.Zone
                
            """)
            
    )

taxiRidesDF.orderBy("Borough", "Zone").show(truncate=False)

taxiRidesDF.createOrReplaceTempView("TaxiRides")
    
# Calculate total rides across each borough
a) Create Window over entire table , and partition by Borough
b) Add total rides (across all zones in a borough) against each row.

taxiRidesWindowDF = (
      spark.sql ("""
                   SELECT *
                      , SUM (RideCount) OVER(PARTITION BY Borough) AS TotalRideCountByBorough
                      
                      FROM TaxiRides
               """)
         )
taxiRidesWindowDF.orderBy("Borough", "Zone").show(truncate=False)

taxiRidesWindowDF.createOrReplaceTempView("TaxiRidesWindow")

 # Find share of each zone in terms of rides , within their borough
- Divide Zone's ride count with Borough Ride count.

 
( spark.sql ("""
                   SELECT *
                      , ROUND(RideCount * 100) / TotalRideCountByBorough, 2) AS RidesSharePercentInBorough
                      
                      FROM TaxiRidesWindow
                      ORDER BY Borough , Zone
         """)).show(truncate=False)
         
         
         )
     


                   






    
          


      








 
 
 












       

















#================================================================================================================================================
#What is apache Spark Shuffling?
#Apache Spark is an open source data processing framework widely used for large-scale data processing and analysis.
# It was designed to be easy to use while providing high performance for batchs and streaming data. 
#Spark’s distributed architecture is one of its key features, allowing it to scale and process data across multiple clusters of machines in parallel.

#Because it can distribute the processing load among multiple machines and processors, it is ideal for processing large amounts of data.
#Spark Framework is modular, with a core engine that manages task dispatching, scheduling, and execution, and a collection of libraries
# that offer a range of functions for machine learning, data processing, and other activities.

#The driver program, which controls how Spark applications are executed, is the brains of the Spark engine.
# Usually running on a single host, a driver application breaks down the large tasks into smaller ones that may be sent to the worker nodes 
#in the cluster.

#In a Spark cluster, worker nodes are in charge of carrying out the duties given to them by the spark driver software.

#An executor process is run by each worker node and is in charge of carrying out given tasks.
# The real workers who run calculations on your data are known as executors.

#Spark also has a cluster manager responsible for allocating resources to Spark applications and managing cluster worker nodes.
# Spark works with different cluster managers like “Kubernetes, Hadoop YARN..” in addition it owns standalone cluster manager.
#In summary, the Apache Spark architecture consists of a driver, worker nodes with executors, and a cluster manager,
# all of which work together to efficiently process and analyze large amounts of data.

#Data Shuffling - Shuffling is the process of distributing data across the cluster workers in order to process it in parallel.
# It happens generally when data is not evenly distributed, when data should be arranged in a specific way to be processed or 
#when there is not enough memory on a single node to store all the required data for processing.

#In order that spark ensures that all the records with the same key are on the same node,
# Spark needs to shuffle the data if performing operations like groupBy and joins on a large dataset.
# This makes it possible to process all the records at once and combine the results.

#The shuffle operation must be finished before the next stage of processing can start, which can also delay the processing of the data.

#Shuffling Causes:-
#a) Data skew: Spark shuffling may take place when some keys in a dataset are significantly more heavily populated with data than others.
# Data skew is the term used to describe this.Operations like groupByKey, reduceByKey or same sort of joins which call for the grouping 
#or aggregation of data by key, can also cause data shuffling.

#b) Partitioning: Spark divides data among nodes through a procedure known as partitioning. 
#The possibility of shuffling exists if the data is not distributed among partitions equally.

#c) Spark operations: Groupping, aggregating the data by key or when joining two datasets, some operations, 
#such as groupByKey, reduceByKey and join, will cause shuffling.

#d) Caching : Shuffling may occur if a dataset is cached in memory and the amount of data in the cache exceeds 
#the amount of memory on a single node.

#e)Data locality: Spark tries to minimize shuffling by putting data on the same node as the computation that will be run on it.
# It must be moved to the node where the computation is being done if the data is not already stored there.

#Data shuffling is a performance killer and costly process as it leads to network data transfer by data travel between nodes, 
#High disk I/O and Rearranging files operations. The performance of Spark applications can then be enhanced by being aware 
#of these elements and avoiding shuffling whenever possible.

#From default schema or database "default" 
%sql
show tables

#From a particular database
show tables from test_db

#Table Metadata

describe customersalesdataset_csv

desc ipl_2013_csv

%sql
desc test_db.ipl_2013_csv

%Python
#To mount data.
%run "./Includes/Classroom-Setup"

%run  "./Includes/Utility-Methods"

#Our entry point for Spark 2.0 applications is the class SparkSession.
#An instance of this object is already instantiated for us which can be easily demonstrated by running the next cell:

print(spark)

#It's worth noting that in Spark 2.0 SparkSession is a replacement for the other entry points:

#SparkContext, available in our notebook as sc.
#SQLContext, or more specifically it's subclass HiveContext, available in our notebook as sqlContext.

print(sc)
print(sqlContext)

#SPARKSESSION
#Quick function review:

#createDataSet(..)
#createDataFrame(..)
#emptyDataSet(..)
#emptyDataFrame(..)
#range(..)
#read(..)
#readStream(..)
#sparkContext(..)
#sqlContext(..)
#sql(..)
#streams(..)
#table(..)
#udf(..)
#The function we are most interested in is SparkSession.read() which returns a DataFrameReader.

#DataFrameReader
#Quick function review:
#csv(path)
#jdbc(url, table, ..., connectionProperties)
#json(path)
#format(source)
#load(path)
#orc(path)
#parquet(path)
#table(tableName)
#text(path)
#textFile(path)

#Configuration methods:

#option(key, value)
#options(map)
#schema(schema)

READ CSV
#Reading from CSV w/InferSchema.
-----------------------------------------------------------------------
#We are going to start by reading in a very simple text file.- pageviews_by_second.tsv
#We can use %fs ls ... to view the file on the DBFS.

%fs ls /mnt/training/wikipedia/pageviews/

#We can use %fs head ... to peek at the first couple thousand characters of the file.

%fs head /mnt/training/wikipedia/pageviews/pageviews_by_second.tsv

#There are a couple of things to note here:

#The file has a header.
#The file is tab separated (we can infer that from the file extension and the lack of other characters between each "column").
#The first two columns are strings and the third is a number.
#Knowing those details, we can read in the "CSV" file.

Step #1 - Read The CSV File
#Let's start with the bare minimum by specifying the tab character as the delimiter and the location of the file:

# A reference to our tab-separated-file
csvFile = "/mnt/training/wikipedia/pageviews/pageviews_by_second.tsv"

tempDF = (spark.read           # The DataFrameReader
   .option("sep", "\t")        # Use tab delimiter (default is comma-separator)
   .csv(csvFile)               # Creates a DataFrame from CSV after reading in the file
)

#This is guaranteed to trigger one job.
#A Job is triggered anytime we are "physically" required to touch the data.
#In some cases, one action may create multiple jobs (multiple reasons to touch the data).
#In this case, the reader has to "peek" at the first line of the file to determine how many columns of data we have.

#We can see the structure of the DataFrame by executing the command printSchema()

#It prints to the console the name of each column, its data type and if it's null or not.

#** Note: ** We will be covering the other DataFrame functions in other notebooks.

#We can see from the schema that...
#there are three columns
#the column names _c0, _c1, and _c2 (automatically generated names)
#all three columns are strings
#all three columns are nullable
#And if we take a quick peek at the data, we can see that line #1 contains the headers and not data:

display(tempDF)

#Step #2 - Use the File's Header

#Next, we can add an option that tells the reader that the data contains a header and to use that header to determine our column names.

#** NOTE: ** We know we have a header based on what we can see in "head" of the file from earlier.

(spark.read                    # The DataFrameReader
   .option("sep", "\t")        # Use tab delimiter (default is comma-separator)
   .option("header", "true")   # Use first line of all files as header
   .csv(csvFile)               # Creates a DataFrame from CSV after reading in the file
   .printSchema()
)

#A couple of notes about this iteration:

#again, only one job
#there are three columns
#all three columns are strings
#all three columns are nullable
#the column names are specified: timestamp, site, and requests (the change we were looking for)
#A "peek" at the first line of the file is all that the reader needs to determine the number of columns and the name of each column.

#Before going on, make a note of the duration of the previous call - it should be just under 3 seconds.

#Step #3 - Infer the Schema
#Lastly, we can add an option that tells the reader to infer each column's data type (aka the schema)

(spark.read                        # The DataFrameReader
   .option("header", "true")       # Use first line of all files as header
   .option("sep", "\t")            # Use tab delimiter (default is comma-separator)
   .option("inferSchema", "true")  # Automatically infer data types
   .csv(csvFile)                   # Creates a DataFrame from CSV after reading in the file
   .printSchema()
)

#Reading from CSV w/User-Defined Schema.
------------------------------------------------------------------------------------------------------------------------
This time we are going to read the same file.
The difference here is that we are going to define the schema beforehand and hopefully avoid the execution of any extra jobs.

Step #1
#Declare the schema.

#This is just a list of field names and data types.



Step #2
#Read in our data (and print the schema).

#We can specify the schema, or rather the StructType, with the schema(..) command:

(spark.read                   # The DataFrameReader
  .option('header', 'true')   # Ignore line #1 - it's a header
  .option('sep', "\t")        # Use tab delimiter (default is comma-separator)
  .schema(csvSchema)          # Use the specified schema
  .csv(csvFile)               # Creates a DataFrame from CSV after reading in the file
  .printSchema()
)

#Let's take a look at some of the other details of the DataFrame we just created for comparison sake.

csvDF = (spark.read
  .option('header', 'true')
  .option('sep', "\t")
  .schema(csvSchema)
  .csv(csvFile)
)
print("Partitions: " + str(csvDF.rdd.getNumPartitions()) )
printRecordsPerPartition(csvDF)
print("-"*80)

READ JSON
#Run the following cell to configure our "classroom."
%run "./Includes/Classroom-Setup"
%run "./Includes/Utility-Methods"

#Reading from JSON w/ InferSchema
#Reading in JSON isn't that much different than reading in CSV files.

#Let's start with taking a look at all the different options that go along with reading in JSON files.

#JSON Lines
#Much like the CSV reader, the JSON reader also assumes...

#That there is one JSON object per line and...
#That it's delineated by a new-line.
#This format is referred to as JSON Lines or newline-delimited JSON

#More information about this format can be found at http://jsonlines.org.

#** Note: ** Spark 2.2 was released on July 11th 2016. With that comes File IO improvements for CSV & JSON, but more importantly, Support for parsing multi-line JSON and CSV files.
 #You can read more about that (and other features in Spark 2.2) in the Databricks Blog.
 
 #The Data Source
#For this exercise, we will be using the file called snapshot-2016-05-26.json (4 MB file from Wikipedia).
#The data represents a set of edits to Wikipedia articles captured in May of 2016.
#It's located on the DBFS at dbfs:/mnt/training/wikipedia/edits/snapshot-2016-05-26.json
#Like we did with the CSV file, we can use %fs ls ... to view the file on the DBFS.

%fs ls dbfs:/mnt/training/wikipedia/edits/snapshot-2016-05-26.json

%md
#Like we did with the CSV file, we can use **&percnt;fs head ...** to peek at the first couple lines of the JSON file.

%fs head dbfs:/mnt/training/wikipedia/edits/snapshot-2016-05-26.json

#Read The JSON File
#The command to read in JSON looks very similar to that of CSV.
#In addition to reading the JSON file, we will also print the resulting schema.

jsonFile = "dbfs:/mnt/training/wikipedia/edits/snapshot-2016-05-26.json"

wikiEditsDF = (spark.read           # The DataFrameReader
    .option("inferSchema", "true")  # Automatically infer data types & column names
    .json(jsonFile)                 # Creates a DataFrame from JSON after reading in the file
 )
wikiEditsDF.printSchema()

#With our DataFrame created, we can now take a peak at the data.

#But to demonstrate a unique aspect of JSON data (or any data with embedded fields),
 #we will first create a temporary view and then view the data via SQL:
 
 # create a view called wiki_edits
wikiEditsDF.createOrReplaceTempView("wiki_edits")

#And now we can take a peak at the data with simple SQL SELECT statement:

%sql

SELECT * FROM wiki_edits 

#Notice the geocoding column has embedded data.

#You can expand the fields by clicking the right triangle in each row.

#But we can also reference the sub-fields directly as we see in the following SQL statement:

%sql

SELECT channel, page, geocoding.city, geocoding.latitude, geocoding.longitude 
FROM wiki_edits 
WHERE geocoding.city IS NOT NULL

#While there are similarities between reading in CSV & JSON there are some key differences:

#We only need one job even when inferring the schema.
#There is no header which is why there isn't a second job in this case - the column names are extracted from the JSON object's attributes.
#Unlike CSV which reads in 100% of the data, the JSON reader only samples the data.
#Note: In Spark 2.2 the behavior was changed to read in the entire JSON file.

#Reading from JSON w/ User-Defined Schema
#To avoid the extra job, we can (just like we did with CSV) specify the schema for the DataFrame.

Step #1 - Create the Schema
#Compared to our CSV example, the structure of this data is a little more complex.
#Note that we can support complex data types as seen in the field geocoding.


Step #2 - Read in the JSON
#Next, we will read in the JSON file and once again print its schema.

(spark.read            # The DataFrameReader
  .schema(jsonSchema)  # Use the specified schema
  .json(jsonFile)      # Creates a DataFrame from JSON after reading in the file
  .printSchema()
)

#Review: Reading from JSON w/ User-Defined Schema
#Just like CSV, providing the schema avoids the extra jobs.
#The schema allows us to rename columns and specify alternate data types.
#Can get arbitrarily complex in its structure.

#Let's take a look at some of the other details of the DataFrame we just created for comparison sake.

jsonDF = (spark.read
  .schema(jsonSchema)
  .json(jsonFile)    
)
print("Partitions: " + str(jsonDF.rdd.getNumPartitions()))
printRecordsPerPartition(jsonDF)
print("-"*80)

#And of course we can view that data here:
display(jsonDF)

#Reading from Parquet Files
#"Apache Parquet is a columnar storage format available to any project in the Hadoop ecosystem, 
#regardless of the choice of data processing framework, data model or programming language."

#About Parquet Files
#Free & Open Source.
#Increased query performance over row-based data stores.
#Provides efficient data compression.
#Designed for performance on large data sets.
#Supports limited schema evolution.
#Is a splittable "file format".
#A Column-Oriented data store

%fs ls /mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/

#Unlike our CSV and JSON example, the parquet "file" is actually 11 files, 
#8 of which consist of the bulk of the data and the other three consist of meta-data.

#Read in the Parquet Files
#To read in this files, we will specify the location of the parquet directory.

parquetFile = "/mnt/training/wikipedia/pageviews/pageviews_by_second.parquet/"

(spark.read              # The DataFrameReader
  .parquet(parquetFile)  # Creates a DataFrame from Parquet after reading in the file
  .printSchema()         # Print the DataFrame's schema
)

#Review: Reading from Parquet Files
#We do not need to specify the schema - the column names and data types are stored in the parquet files.
#Only one job is required to read that schema from the parquet file's metadata.
#Unlike the CSV or JSON readers that have to load the entire file and then infer the schema,
 #the parquet reader can "read" the schema very quickly because it's reading that schema from the metadata.
 
 #Read in the Parquet Files w/Schema
#If you want to avoid the extra job entirely, we can, again, specify the schema even for parquet files:

#** WARNING ** Providing a schema may avoid this one-time hit to determine the DataFrame's schema.
#However, if you specify the wrong schema it will conflict with the true schema and will result in an analysis exception at runtime.
# Required for StructField, StringType, IntegerType, etc.
from pyspark.sql.types import *

parquetSchema = StructType(
  [
    StructField("timestamp", StringType(), False),
    StructField("site", StringType(), False),
    StructField("requests", IntegerType(), False)
  ]
)

(spark.read               # The DataFrameReader
  .schema(parquetSchema)  # Use the specified schema
  .parquet(parquetFile)   # Creates a DataFrame from Parquet after reading in the file
  .printSchema()          # Print the DataFrame's schema
)

#Let's take a look at some of the other details of the DataFrame we just created for comparison sake.

parquetDF = spark.read.schema(parquetSchema).parquet(parquetFile)

print("Partitions: " + str(parquetDF.rdd.getNumPartitions()) )
printRecordsPerPartition(parquetDF)
print("-"*80)

#In most/many cases, people do not provide the schema for Parquet files because reading in the schema is such a cheap process.
#And lastly, let's peek at the data:

display(parquetDF)

#Reading Data - Tables and Views


#Follow these steps to register a new Table
#NOTE: It may be easiest for you to duplicate this browser tab so you can refer back to these steps.

#1.Download the pageviews_by_second_example.tsv file to your computer.
#2.Select Data in the left-hand menu.
#3.Select the database with your username.
#4.Select Add Data to create a new Table.
#5.In the Create New Table form, make sure Upload File is selected, 
#then click on browse and select the pageviews_by_second_example.tsv file is highlighted, or drag and drop it into the File box.
#6.Select Create Table with UI.
#7.Select your cluster, then select Preview Table.
#8.Under Create in Database, select the database with your username in the list. It is important that you do not skip this step.
# You can find the database name in the output of cell 3 above.
#9.Select Create Table.

#Reading from a Table/View
#We can now read in the "table" pageviews_by_seconds_example as a DataFrame
# with one simple command (and then print the schema):

pageviewsBySecondsExampleDF = spark.read.table("pageviews_by_second_example_tsv")

pageviewsBySecondsExampleDF.printSchema()

#And of course we can now view that data as well:
display(pageviewsBySecondsExampleDF)

#Review: Reading from Tables
#No job is executed - the schema is stored in the table definition on Databricks.
#The data types shown here are those we defined when we registered the table.
#In our case, the file was uploaded to Databricks and is stored on the DBFS.
#If we used JDBC, it would open the connection to the database and read it in.
#If we used an object store (like what is backing the DBFS), it would read the data from source.
#The "registration" of the table simply makes future access, or access by multiple users easier.
#The users of the notebook cannot see username and passwords, secret keys, tokens, etc.

#Let's take a look at some of the other details of the DataFrame we just created for comparison sake.

 print("Partitions: " + str(pageviewsBySecondsExampleDF.rdd.getNumPartitions()))
printRecordsPerPartition(pageviewsBySecondsExampleDF)
print("-"*80)

#Temporary Views
#Tables that are loadable by the call spark.read.table(..) are also accessible through the SQL APIs.

#For example, we already used Databricks to expose pageviews_by_second_example_tsv as a table/view

%sql
select * from pageviews_by_second_example_tsv limit(5)

#You can also take an existing DataFrame and register it as a view exposing it as a table to the SQL API.
#If you recall from earlier, we have an instance called parquetDF.
#We can create a [temporary] view with this call...
 
 # create a DataFrame from a parquet file
parquetFile = "/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/"
parquetDF = spark.read.parquet(parquetFile)

# create a temporary view from the resulting DataFrame
parquetDF.createOrReplaceTempView("parquet_table")

#And now we can use the SQL API to reference that same DataFrame as the table parquet_table.
%sql
select * from parquet_table order by requests desc limit(5)

#Note #1: ** The method createOrReplaceTempView(..) is bound to the SparkSession meaning it will be discarded once the session ends.

#Note #2: ** On the other hand, the method createOrReplaceGlobalTempView(..) is bound to the spark application.*

#Or to put that another way, I can use createOrReplaceTempView(..) in this notebook only.
#However, I can call createOrReplaceGlobalTempView(..) in this notebook and then access it from another.

#   Writing Data
#Just as there are many ways to read data, we have just as many ways to write data.

#In this notebook, we will take a quick peek at how to write data back out to Parquet files.

#Writing Data
#Let's start with one of our original CSV data sources, pageviews_by_second.tsv:

from pyspark.sql.types import *

csvSchema = StructType([
  StructField("timestamp", StringType(), False),
  StructField("site", StringType(), False),
  StructField("requests", IntegerType(), False)
])

csvFile = "/mnt/training/wikipedia/pageviews/pageviews_by_second.tsv"

csvDF = (spark.read
  .option('header', 'true')
  .option('sep', "\t")
  .schema(csvSchema)
  .csv(csvFile)
)

#Now that we have a DataFrame, we can write it back out as Parquet files or other various formats.

fileName = userhome + "/pageviews_by_second.parquet"
print("Output location: " + fileName)

(csvDF.write                       # Our DataFrameWriter
  .option("compression", "snappy") # One of none, snappy, gzip, and lzo
  .mode("overwrite")               # Replace existing files
  .parquet(fileName)               # Write DataFrame to Parquet files
)

#Now that the file has been written out, we can see it in the DBFS:

display(
  dbutils.fs.ls(fileName)
)

#And lastly we can read that same parquet file back in and display the results:

display(
  spark.read.parquet(fileName)
)

#The goal of this lab is to put into practice some of what you have learned about reading data with Apache Spark.
#The instructions are provided below along with empty cells for you to do your work.
#At the bottom of this notebook are additional cells that will help verify that your work is accurate.

#Instructions
#Start with the file dbfs:/mnt/training/wikipedia/clickstream/2015_02_clickstream.tsv, some random file you haven't seen yet.
#Read in the data and assign it to a DataFrame named testDF.
#Run the last cell to verify that the data was loaded correctly and to print its schema.
#The one untestable requirement is that you should be able to create the DataFrame and print its schema without executing a single job.
#Note: For the test to pass, the following columns should have the specified data types:

#prev_id: integer
#curr_id: integer
#n: integer
#prev_title: string
#curr_title: string
#type: string


# TODO

fileName = "dbfs:/mnt/training/wikipedia/clickstream/2015_02_clickstream.tsv"

testDF = <<FILL_IN>>

#Run the following cell to verify that your DataFrame was created properly.

#Remember: This should execute without triggering a single job.

testDF.printSchema()

columns = testDF.dtypes
assert len(columns) == 6, "Expected 6 columns but found " + str(len(columns))

assert columns[0][0] == "prev_id",    "Expected column 0 to be \"prev_id\" but found \"" + columns[0][0] + "\"."
assert columns[0][1] == "int",        "Expected column 0 to be of type \"int\" but found \"" + columns[0][1] + "\"."

assert columns[1][0] == "curr_id",    "Expected column 1 to be \"curr_id\" but found \"" + columns[1][0] + "\"."
assert columns[1][1] == "int",        "Expected column 1 to be of type \"int\" but found \"" + columns[1][1] + "\"."

assert columns[2][0] == "n",          "Expected column 2 to be \"n\" but found \"" + columns[2][0] + "\"."
assert columns[2][1] == "int",        "Expected column 2 to be of type \"int\" but found \"" + columns[2][1] + "\"."

assert columns[3][0] == "prev_title", "Expected column 3 to be \"prev_title\" but found \"" + columns[3][0] + "\"."
assert columns[3][1] == "string",     "Expected column 3 to be of type \"string\" but found \"" + columns[3][1] + "\"."

assert columns[4][0] == "curr_title", "Expected column 4 to be \"curr_title\" but found \"" + columns[4][0] + "\"."
assert columns[4][1] == "string",     "Expected column 4 to be of type \"string\" but found \"" + columns[4][1] + "\"."

assert columns[5][0] == "type",       "Expected column 5 to be \"type\" but found \"" + columns[5][0] + "\"."
assert columns[5][1] == "string",     "Expected column 5 to be of type \"string\" but found \"" + columns[5][1] + "\"."

print("Congratulations, all tests passed... that is if no jobs were triggered :-)\n")


#Creating user specific database.

1. %scala
val databaseName = {
  val tags = com.databricks.logging.AttributionContext.current.tags
  val name = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
  val username = if (name != "unknown") name else dbutils.widgets.get("databricksUsername")
  val databaseName   = username.replaceAll("[^a-zA-Z0-9]", "_") + "_db"
  spark.conf.set("com.databricks.training.spark.databaseName", databaseName)
  databaseName
}
displayHTML(s"Created user-specific database")

#create a database.
2. %scala
spark.sql("CREATE DATABASE IF NOT EXISTS `%s`".format(databaseName))
spark.sql("USE `%s`".format(databaseName))

displayHTML("""Using the database <b style="color:green">%s</b>.""".format(databaseName))


-- Create database `customer_db`. This throws exception if database with name customer_db
-- already exists.
CREATE DATABASE customer_db;

-- Create database `customer_db` only if database with same name doesn't exist.
CREATE DATABASE IF NOT EXISTS customer_db;

-- Create database `customer_db` only if database with same name doesn't exist with 
-- `Comments`,`Specific Location` and `Database properties`.
CREATE DATABASE IF NOT EXISTS customer_db COMMENT 'This is customer database' LOCATION '/user'
    WITH DBPROPERTIES (ID=001, Name='John');

-- Verify that properties are set.
DESCRIBE DATABASE EXTENDED customer_db;
+-------------------------+--------------------------+
|database_description_item|database_description_value|
+-------------------------+--------------------------+
|            Database Name|               customer_db|
|              Description| This is customer database|
|                 Location|     hdfs://hacluster/user|
|               Properties|   ((ID,001), (Name,John))|
+-------------------------+--------------------------+


File path: /FileStore/tables/my_directory_2024/


# Import SparkSession
from pyspark.sql import SparkSession

# Create SparkSession 
spark = SparkSession.builder \
      .master("local[2]") \
      .appName("SparkByExamples.com") \
      .getOrCreate()
      
# **** In most of the tools, notebooks, and Azure Databricks,
 #the environment creates a default SparkSession object for us to use,so you don’t have to worry about creating a Spark session.
 
#master() – This allows Spark applications to connect and run in different modes (local, standalone cluster, Mesos, YARN), depending on the configuration.

#Use local[x] when running on your local laptop. x should be an integer value and should be greater than 0; this represents how many partitions it should create when using RDD, DataFrame, and Dataset.
# Ideally, x value should be the number of CPU cores you have.
#For standalone use spark://master:7077
#appName() – Sets a name to the Spark application that shows in the Spark web UI. If no application name is set, it sets a random name.

#getOrCreate() – This returns a SparkSession object if it already exists. Creates a new one if it does not exist.

#Sometimes,you might be required to create multiple sessions,which you can easily achieve by using newSession() method.
# This uses the same app name and master as the existing session. Underlying SparkContext will be the same for both sessions,
# as you can have only one context per Spark application.

// Create a new SparkSession
val spark3 = spark.newSession()
print(spark3)

#Compare this hash with the hash from the above example; it should be different.
      
#Spark version -- 
print ({spark.version})
#List tables in a database.
spark.catalog.listTables('test_db')
spark.catalog.listDatabases()

#Using another schema / database.
%sql
Use schema test_db

#Now testing.
%sql
select current_database()


      
# SQL Select query
spark.sql("SELECT `PLAYER NAME`,AGE,COUNTRY,TEAM,`PLAYING ROLE`,`T-RUNS`,`T-WKTS` FROM test_db.ipl_2013_csv") \
     .show(5)
     
# SQL Select query
spark.sql("SELECT `PLAYER NAME`,AGE,COUNTRY,TEAM,`PLAYING ROLE`,`T-RUNS`,`T-WKTS` FROM test_db.ipl_2013_csv where COUNTRY='IND'")\
     .show(100)
     
     spark.sql("SELECT `PLAYER NAME` ,AGE,COUNTRY,TEAM,`PLAYING ROLE`,`T-RUNS`,`T-WKTS` FROM test_db.ipl_2013_csv where COUNTRY in ('IND','AUS','SA') ORDER BY COUNTRY")\
    .show(100)
    
 # PySpark Update
 
 from pyspark.sql import SparkSession
spark = SparkSession.builder \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()

data = [('James','Smith','M',3000), ('Anna','Rose','F',4100),
  ('Robert','Williams','M',6200)
]
columns = ["firstname","lastname","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
df.show()

#A new dataframe gets created.
df2=df.withColumn("salary", df.salary*3)
df2.show()

#Update column based on condition.
from pyspark.sql.functions import when
df3 = df.withColumn("gender", when(df.gender == "M","Male") \
      .when(df.gender == "F","Female") \
      .otherwise(df.gender))
df3.show()

#Update DataFrame column data type
df4=df.withColumn("salary", df.salary.cast("String"))
df4.printSchema ()

#Update DataFrame Column with a constant
#The withColumn() method is also used to create a new DataFrame column with a constant value using the lit function. Below is an example.

df.withColumn("state",lit("CA")).show()

#PySpark SQL update
df.createOrReplaceTempView("PER")
df5=spark.sql("select firstname,gender,salary*3 as salary from PER")
df5.show()


Quickstart: DataFrame
#This is a short introduction and quickstart for the PySpark DataFrame API.
#PySpark DataFrames are lazily evaluated. They are implemented on top of RDDs.
#When Spark transforms data, it does not immediately compute the transformation but plans how to compute later.
#When actions such as collect() are explicitly called, the computation starts. 

#PySpark applications start with initializing SparkSession which is the entry point of PySpark as below.
#In case of running it in PySpark shell via pyspark executable, the shell automatically creates the session in the variable spark for users.

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


#The DataFrames created above all have the same results and schema.
# All DataFrames above result same.
df.show()
df.printSchema()

#Viewing Data
#The top rows of a DataFrame can be displayed using DataFrame.show().

df.show(1)

#Alternatively, you can enable spark.sql.repl.eagerEval.enabled configuration for the eager evaluation of PySpark DataFrame in notebooks 
#such as Jupyter.#The number of rows to show can be controlled via spark.sql.repl.eagerEval.maxNumRows configuration.
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)
df.show()   

#The rows can also be shown vertically. This is useful when rows are too long to show horizontally.

df.show(1, vertical=True)

#You can see the DataFrame's schema and column names as follows:
df.columns
df.printSchema()

#Show the summary of the DataFrame.
df.select("a", "b", "c").describe().show()

#DataFrame.collect() collects the distributed data to the driver side as the local data in Python.
# Note that this can throw an out-of-memory error when the dataset is too large to fit in the driver side because it collects all the data from executors to the driver side.
df.collect()

#In order to avoid throwing an out-of-memory exception, use DataFrame.take() or DataFrame.tail().
df.take(1)

#PySpark DataFrame also provides the conversion back to a pandas DataFrame to leverage pandas API.
# Note that toPandas also collects all data into the driver side that can easily cause an out-of-memory-error
#when the data is too large to fit into the driver side.

df.toPandas()

## Selecting and Accessing Data

#PySpark DataFrame is lazily evaluated and simply selecting a column does not trigger the computation but it returns a `Column` instance.

df.a

from pyspark.sql import Column
from pyspark.sql.functions import upper

type(df.c) == type(upper(df.c)) == type(df.c.isNull())

#These Columns can be used to select the columns from a DataFrame.
# For example, DataFrame.select() takes the Column instances that returns another DataFrame.

df.select(df.c).show()

#Assign new Column instance.
df.withColumn('upper_c', upper(df.c)).show()

#To select a subset of rows, use DataFrame.filter().
df.filter(df.a == 1).show()

#Applying a Function
#PySpark supports various UDFs and APIs to allow users to execute Python native functions.
#See also the latest Pandas UDFs and Pandas Function APIs. For instance, the example below allows users to directly use the APIs 
#in a pandas Series within Python native function.
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf('long')
def pandas_plus_one(series: pd.Series) -> pd.Series:
    # Simply plus one by using pandas Series.
    return series + 1

df.select(pandas_plus_one(df.a)).show() 

#Another example is DataFrame.mapInPandas which allows users directly use the APIs in a pandas DataFrame 
#without any restrictions such as the result length.

def pandas_filter_func(iterator):
    for pandas_df in iterator:
        yield pandas_df[pandas_df.a == 1]

df.mapInPandas(pandas_filter_func, schema=df.schema).show()


#Grouping Data
#PySpark DataFrame also provides a way of handling grouped data by using the common approach,
# split-apply-combine strategy. It groups the data by a certain condition applies a function to each group 
#and then combines them back to the DataFrame.

df = spark.createDataFrame([
    ['red', 'banana', 1, 10], ['blue', 'banana', 2, 20], ['red', 'carrot', 3, 30],
    ['blue', 'grape', 4, 40], ['red', 'carrot', 5, 50], ['black', 'carrot', 6, 60],
    ['red', 'banana', 7, 70], ['red', 'grape', 8, 80]], schema=['color', 'fruit', 'v1', 'v2'])
df.show()

#Grouping and then applying the avg() function to the resulting groups.
df.groupby('color').avg().show()

#You can also apply a Python native function against each group by using pandas API.
def plus_mean(pandas_df):
    return pandas_df.assign(v1=pandas_df.v1 - pandas_df.v1.mean())

df.groupby('color').applyInPandas(plus_mean, schema=df.schema).show()

#Co-grouping and applying a function.
df1 = spark.createDataFrame(
    [(20000101, 1, 1.0), (20000101, 2, 2.0), (20000102, 1, 3.0), (20000102, 2, 4.0)],
    ('time', 'id', 'v1'))

df2 = spark.createDataFrame(
    [(20000101, 1, 'x'), (20000101, 2, 'y')],
    ('time', 'id', 'v2'))

def merge_ordered(l, r):
    return pd.merge_ordered(l, r)

df1.groupby('id').cogroup(df2.groupby('id')).applyInPandas(
    merge_ordered, schema='time int, id int, v1 double, v2 string').show()
    
# Getting Data In/Out
#CSV is straightforward and easy to use. Parquet and ORC are efficient and compact file formats to read and write faster.

#There are many other data sources available in PySpark such as JDBC, text, binaryFile, Avro, etc.
# See also the latest Spark SQL, DataFrames and Datasets Guide in Apache Spark documentation.

#CSV
df.write.csv('foo.csv', header=True)
spark.read.csv('foo.csv', header=True).show()

#Parquet
df.write.parquet('bar.parquet')
spark.read.parquet('bar.parquet').show()

#ORC
df.write.orc('zoo.orc')
spark.read.orc('zoo.orc').show()

#Working with SQL
#DataFrame and Spark SQL share the same execution engine so they can be interchangeably used seamlessly.
#For example, you can register the DataFrame as a table and run a SQL easily as below:

df.createOrReplaceTempView("tableA")
spark.sql("SELECT count(*) from tableA").show()

#In addition, UDFs can be registered and invoked in SQL out of the box:
@pandas_udf("integer")
def add_one(s: pd.Series) -> pd.Series:
    return s + 1

spark.udf.register("add_one", add_one)
spark.sql("SELECT add_one(v1) FROM tableA").show()

#These SQL expressions can directly be mixed and used as PySpark columns.
from pyspark.sql.functions import expr

df.selectExpr('add_one(v1)').show()
df.select(expr('count(*)') > 0).show()

1
#Lazy evaluation
#Lazy evaluation, also known as call-by-need, is a technique where an expression is only evaluated when its value is needed.
#This means that the computation is deferred until the last possible moment, and the result is cached for future use.
#Lazy evaluation can avoid unnecessary work, save memory, and enable infinite data structures.
# However, it can also introduce overhead, complicate debugging, and affect performance unpredictably.
# Some programming languages that support lazy evaluation are Haskell, Scala, Clojure, and Python.

2
#Eager evaluation
#Eager evaluation, also known as call-by-value, is a technique where an expression is evaluated as soon as it is bound to a variable
#or passed as an argument. This means that the computation is performed upfront, and the result is stored in memory.
# Eager evaluation can simplify reasoning, improve efficiency, and facilitate parallelism.
# However, it can also waste resources, cause side effects, and limit expressiveness.
#Some programming languages that use eager evaluation are C, Java, Ruby, and JavaScript.


#Spark Optimization techniques.
#We can broadly categorize optimization techniques into three categories as given below:

#Optimizing Spark configurations: This includes changing the Spark properties.
#Optimizing Spark program: This includes code-level optimizations.
#Optimizing storage: This includes file format optimization.

#1)Define the schema beforehand and hopefully avoid the execution of any extra jobs.
#For a small file, manually creating the the schema may not be worth the effort.
#However, for a large file, the time to manually create the schema may be worth the trade off of a really long infer-schema process.
# 2) Use Parquet -  
#Free & Open Source.
#Increased query performance over row-based data stores.
#Provides efficient data compression.
#Designed for performance on large data sets.
#Supports limited schema evolution.
#Is a splittable "file format".
#A Column-Oriented data store

#Only one job is required to read that schema from the parquet file's metadata.
#Unlike the CSV or JSON readers that have to load the entire file and then infer the schema,
 #the parquet reader can "read" the schema very quickly because it's reading that schema from the metadata.
 
 
 The preferred option while reading any file would be to enforce a custom schema. 
 This ensures that the data types are consistent and avoids any unexpected behavior.

In order to do that, you first declare the schema to be enforced, and then read the data by setting schema option.

csvSchema = StructType([StructField(“id",IntegerType(),False)])
df=spark.read.format("csv").schema(csvSchema).load(filePath)
As a result of predefining the schema for your data, you avoid triggering any jobs. 
Spark didn’t peek into the file because we took care of the schema. This is known as lazy evaluation ,which is a crucial optimization technique in Spark.
 
 #3) Tuning Spark Executor -> Most of the time, we allocate static resources to the application. This strategy works well when we know our application's source 
 #data size. But what if some day you are getting massive data in source and the other day you are getting small data? Static allocation 
 #will not be a practical choice in this scenario. We can dynamically add/remove executors according to our Spark application's workload.
 #Spark has a dynamic allocation technique to enable executor scaling. This approach is best when we have an inconsistent workload daily. 
 #You need to set the spark.dynamicAllocation.enabled property to "true" to enable the dynamic allocation of resources. 
 #After that Spark application will scale in and scale out executors according to its need.
 
 #Create an object and set the configuration value in the config() function.

#spark = (SparkSession.builder
   #        .master("yarn")
    #       .appName("test") 
     #     .config("spark.sql.shuffle.partitions", 200)
      #     .getOrCreate())
 
 #This requires any one of the below two properties to be set:

#spark.shuffle.service.enabled
#spark.dynamicAllocation.shuffle tracking.enabled

#The following properties are also relevant to dynamic allocation configuration:

#spark.dynamicAllocation.minExecutors
#spark.dynamicAllocation.maxExecutors
#spark.dynamicAllocation.initialExecutors
#spark.dynamicAllocation.executorAllocationRatio

#4) Tuning Spark memory -> We often get an out-of-memory error either at the driver or executor sides. 
#To avoid that, we need a correct memory configuration. The executor memory is divided into different layers, which can be tuned to improve performance. 
#We need to set the spark.memory.fraction property to tune the values of these layers.

#i)Reserved Memory: Memory reserved by the system and its size is hard coded. It is generally 300 MB.
#ii)User Memory: User memory is calculated as ("Java Heap Memory" — "Reserved Memory") * (1.0 — spark.memory.fraction). This memory pool remains after the allocation of Spark Memory, and it is completely up to you to use it in the way you like. 
#User Memory, and it's completely up to you what would be stored in this RAM and how Spark makes completely no accounting on what you do there 
#and whether you respect this boundary or not. Not respecting this boundary in your code might cause an out-of-memory error.
#iii)Spark Memory: Spark memory is calculated as ("Java Heap Memory" — "Reserved Memory") * spark.memory.fraction. 
#Memory pool, managed by Spark, is further divided into two memory regions — Storage Memory & Execution Memory.

#Storage memory is used for storing all of the cached data, and broadcast variables are also stored here.
 #Spark will store that data in this segment for any persist() option that includes 'MEMORY'. It is calculated by using the formula given below:

#Storage Memory = (Java Heap Memory — Reserved Memory) * spark.memory.fraction * spark.memory.storageFraction
#Execution memory is used by Spark for objects created during the execution of a task. It is calculated by using the formula given below:

#Execution Memory = (Java Heap Memory — Reserved Memory) * spark.memory.fraction * (1.0 — spark.memory.storageFraction)

#5) Tune Shuffle File Buffer -> Disk access is slower when compared to in-memory data access as it involves a serialization process that takes up time and resources. 
#We can reduce disk I/O costs by introducing a shuffle read/write file buffer in the memory.
#The memory buffer size controls the disk seeks and system calls made in creating intermediate shuffle files. 
#We need to set the spark.shuffle.file.buffer property to change the memory buffer size. 
#The default value for this property is 32k.

#If the available memory resources are sufficient, we can increase the size of this parameter to 
#reduce the number of times the disk file overflows during the shuffle write process, reducing the number of disk IO times 
#and improving performance. The recommended size for this property is 1 MB. This allows Spark to do more buffering before 
#writing the final map results to disk. Set property as spark.shuffle.file.buffer = 1 MB

#6) Tune Compression block size -> For large datasets, we can change the default compressed block size. 
#These data blocks can be compressed through either storage or speed-based, like LZO, SNAPPY, and GZIP. 
#The below property needs to be set for the block size used in LZ4 compression. The default compression block size is 32 kb which is not optimal for large datasets.
#By increasing block size, you can see up to a 20% reduction in shuffle/spill file size.
#spark.io.compression.lz4.blockSize = 512KB

#7) Tune Shuffle Partitions value -> The shuffle-partition means the number of partitions generated after each transformation step that causes data shuffling,
# such as join(), agg(), reduce(), etc. By setting spark.sql.shuffle.partitions property, 
#you can decide the level of parallelism in your Spark application.

#You can set Spark property spark.sql.shuffle.partitions to control default shuffle partitions. 
#The default value for this property is 200. You should set the number of shuffle partitions according to your cluster's data size 
#and available resources. The number of partitions should be multiple of the executors you have so that partitions can be equally distributed 
#across tasks. spark.sql.shuffle.partitions = <<integer value>>

#8) Broadcast Join -> Let's consider a scenario where you are joining a big table with a small table.
# During this join operation, more shuffling will happen. We can avoid shuffling by using a broadcast join. 
#It will copy a small table to every node where the executor is running.
# However, after a certain threshold, broadcast join tends to have less advantage over shuffle-based joins.
# Example program to illustrate use of broadcast join
from pyspark.sql.functions import broadcast
emp_df = spark.sql("select id, name, dept_id from employee")
dept_df = spark.sql("select dept_id, dept_name from department")
df_joined = emp_df.join(broadcast(dept_df),emp_df.dept_id == dept_df.dept_id, ‘inner’)
df_joined.show(20)

#9) Cache data -> 
#Every time we call the Action in the Spark program, it triggers DAG and executes it from the beginning. 
#That's why it's recommended not to use unnecessary Actions in Spark programs.
# Double-check the code and remove or comment Actions you wrote for debugging/testing in your Spark program.

#One key point to improve performance when the same dataframe is being referred to in multiple places is to cache that dataframe.
# Spark has two functions to cache dataframe: cache() and persist(). RDD's cache() function default saves the dataframe to memory 
#while the persist() function is used to store dataframe at the user-defined storage level. 

#10) Repartition data -> A dataframe is partitioned means there are logical groups of records in it.
# A group of records is called a partition. Each task processes each partition, many tasks run in parallel inside one executor,
# and parallel execution happens in Spark. The level of parallelism can be increased if we distribute data correctly. 
#For repartitioning, Spark has two methods — repartition() and coalesce().

#If we check the source code, we see that coalesce() is called inside the repartition() with the shuffle parameter set to true. 
#The major difference between coalesce() without shuffle parameter and the repartition() is that coalesce() is mainly used to decrease 
#partitions and repartition() is used to increase partitions. Less shuffling happens when we use coalesce() it compared torepartition(). 
#The repartition() function can also be used to solve data skewness issues.

#So, to improve the performance of your application, you need to either increase or decrease the partitions.
# That totally depends on your data. If you are processing large data and want to create more simultaneous tasks, 
#then you can use the repartition() function to increase the partitions. 
#If you want to re-shuffle the data based on column value, you can also use the repartition() function.
# If you want to reduce the number of output files, then you can use the coalesce() function.
# You can significantly improve the application's performance after applying these changes.

# 11) Filter data in earlier steps -> The key point to improve the performance of joins and other processing is to filter data in earlier steps which you don't need in the result set.
#Let's say you want to process and store only employees in 'Mumbai'. 
#Then instead of reading the whole data and process and then applying filters on the result,
# you can just filter your dataframe earlier while reading the tables into the dataframe.

#There is one more important feature of Spark. It optimizes the logical plan internally.
# It uses predicate pushdown for supported file formats and pushes filters at the beginning, and then it does the processing on filtered data.

#12)Bucketing and Partitioning -> Bucketing and Partitioning are widely used techniques for optimizing hive tables. Partitioning is often represented as directories, and bucketing is represented as files.
# Partitioning splits records into files present under different directories named the same as partition column values, 
#based on the partition column. Bucketing is helpful in further splitting records into different files based on a hashing function.
# This improves performance when we read data in Spark.

#If the table is partitioned and bucketed, then the "Spark SQL" in which filter columns are the same as partitioned/bucketed columns 
#can run faster because it only scans particular directories and files instead of scanning all directories. 
#Thus, query performance is improved.
#The cardinality of a column can be defined as the number of distinct values in that column.
# It is always recommended to use bucketing on high-cardinality columns and partitioning on low-cardinality columns.

#13) Avoid certain operations: such as groupByKey and reduceByKey cause it can trigger shuffling in order to group or aggregate the data.
#14) Use filtering and aggregation instead of groupBy: If you only need to filter the data or perform simple aggregations,
# you can use the filter and agg transformations instead of groupBy.
# These transformations do not trigger shuffling.

#15) Use partitioning on computation engine level : Ensure that the data is evenly distributed across the nodes and .
# You can use the repartition or coalesce to change the number of partitions in a DataFrame.

#16)Use the window function: If you need to perform windowed aggregations, using window functions can be helpful to specify the window 
#and the aggregation function. #The window function does not trigger shuffling, as it operates on the data within each partition.

#17)Use the join transformation: If you are performing a join on two datasets, you can use the join transformation instead of cogroup or groupBy.
# The join transformation does not trigger shuffling, as it relies on data partitioning to ensure 
#that data with the same key is placed in the same partition.



