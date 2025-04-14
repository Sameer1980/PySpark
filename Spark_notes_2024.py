#Hadoop components --> 
#a) Storage Layer - Hadoop Distributed File System (HDFS) . Large size file split into multiple small files and distributed across multiple systems
(nodes).
#b) Processing Layer - Map Reduce Engine - Processing done parallelly using multiple machines in the Hadoop cluster.

#c) Cluster Manager - YARN (Yet Another Resource Manager ),etc. Allocate resources to Spark applications on Cluster for parallel processing.

#Each map reading file is reading and writing to disk. This is known as Disk-heavy architecture. 

#Challenges with Hadoop -- 
#a) Infrastructure & environment complexity
#b) Need to write a lot of code as it uses low-level API.
#c) Disk-heavy architecture- Multiple IO operations slowing down processing.
#d) No built in support for streaming and machine learning.

#Apache SPARK - It is a in-memory engine for distributed data processing that runs on Clusters. It can run workloads upto 100x faster than Hadoop.

#Spark Core is the underlying general execution engine for the Spark platform that all other functionality is built on top of.
#Built on top of Spark, MLlib is a scalable machine learning library that provides a uniform set of high-level APIs 
#that help users create and tune practical machine learning pipelines.
#Spark Streaming(newer version structured streaming) is an extension of the core Spark API that enables scalable, 
#high-throughput, fault-tolerant stream processing of live data streams.

# Open Source connectors -- Relational DB -> SQL Server , Oracle
#          --   NoSQL -> MongoDB, Cassandra , Azure CosmosDB
#          -- Apache Hadoop , HBase , Hive
# ---- Cloud Storages -> ADLS Gen , Amazon S3 
# ---- MPP Engines -> AWS Redshift , Azure Dedicated SQL , Snowflake
# Visualization tools --> PowerBI Tableau
# Streaming -- > Kafka , Azure Event Hubs , AWS Kinesis.

# Cluster Managers -- YARN , Kubernetes , Mesos , etc.
# Distributed File System options -- HDFS , Azure Data Lake Store, Amazon S3 , or Google cloud storage.
# Multiple Language support - Scala , Python , R , SQL & Java. Open source support for C#.

# How execution happens in Spark?
#a) User writes code to create a Spark Application and execute the code.
#b) Spark Application asks Cluster Manager to launch a driver process.(brain of spark application)
#c)Driver process talks to Cluster Manager to allocate resources and create the executer processess.

# Spark application is a set of processes that is allocated resources like CPU, memory , etc.
# Has one driver and multiple executor processes.
#A Cluster has one and only one driver.
# The driver and the executors are Java processes.

# Spark Session is the entry point to all functionality of Spark.
#Use Spark session to read file, create objects, run queries , etc.

# Example -> 1. Read file1.csv from storage , 2. Apply transformations , 3. Write processed data to storage.
#These activities are submitted as job to the Spark Driver process.
# Logically split file1.csv into 4 parts.
# Split job into Tasks. 
# No. of Tasks = No. of partitions
# By above example 4 tasks will be created. 
#=============================================================================================================
#Job-> Job is created when you need to execute code and take action (getting back results)
# Partitions -> A partition is a chunk of data. Driver decides how many partitions to be created . 
# Number of tasks = Number of partitions . Each task processes only one partition. 

# Cores / Threads / Slots -> Since modern day processors are multi threaded one physical core has multiple threads.
# Threads are also called as Slots.  

# Each core can execute only one task at a time.
# Number of parallel tasks = Number of cores.
# Example : Executor size = 2 cores * 14 GB RAM . Suppose there are 4 executors with 2 cores each.
# Total cores = 2 * 4= 8 cores
# Parallel Tasks = Total cores -> 8 Tasks can execute in parallel.

# To read the data Driver has decided to create 16 partitions.
# Total Tasks = Total Partitions -> Driver will create 16 tasks. 

#==================================================================================================================

# RDD -> Resilient Distributed Datasets -> Spark native data structure. RDD represents collection of data in memory.Ex: 
# --When file is loaded in memory it is called RDD.
# --All processing in Spark happens on RDDs.
# --Write code using low-level RDD APIs.
# --APIs load data in memory as RDDs & process them.
# -- Spark does not apply any optimization to RDD code.

# RDDs are in-memory objects , do not have schema , they are distributed collection of elements , all processing in Spark happens on RDDs.

# Features of RDD-
a) In-memory(resides in the memory of cluster)
b) Partitioned - Split into partitions , processed by tasks.
c) Read-only- Transformed into another RDD or result.
d) Resilient - Auto recover in case of failure. Spark will re-execute failed Tasks using their Lineage Graph. 

Read File ----> Split by comma --> City='Delhi'
RDD1               RDD2              RDD3



# DataFrames --> No compile-time safety. Ex: If you apply a string function on numeric data it will not catch it at compile time but throw error 
# at runtime.
# -- Spark applies optimizations to code.
# -- Based on RDDs.

# Datasets --> Based on RDDs.
#-- Provides compile-time safety.
# -- Combination of RDDs and DataFrames.
#-- Spark applies optimizations to code.
#-- Supported in Java & Scala 
                    |----->Strongly Typed APIs --> In Scala & Java / Strictly enforces data types / Compile-time safety
# Structured APIs --|
                    |-----> Untyped APIs --> In Python & R / No compile-time safety/ Casting errors only at runtime.

#For every Spark version corresponsding library versions need to be installed.

#Spark-Submit is a utility to submit a Spark Application / job to a cluster.
#--Code can be in any language - Scala ,Python or JAVA
#--Can be submitted to any supported Cluster Manager.- Local , Standalone, YARN , Mesos , Kubernetes
#-- Provide configuration and dependencies. 
# -- Spark submit is command is used to submit long running jobs on the Cluster.
#-- c:\> spark-submit --help
#./bin/spark-submit --help
#Using --deploy-mode, you specify where to run the PySpark application driver program. Spark support cluster and client deployment modes.

# FROM JUPYTER NOTEBOOK
----------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark=(SparkSession\
        .builder\
         .appName("ExplorationApp")\
         .master("local[4]")\
         .getOrCreate()
         
         )
spark

# TO CHECK:
------------

numbers= [[1],[2],[3],[4],[5]]
numbersDF=spark.createDataFrame( numbers, "Id:int" )
numbersDF.show()

[RuntimeError: Python in worker has different version 3.9 than that in driver 3.10, PySpark cannot run with different minor versions. Please check environment variables PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON are correctly set.

You need to have exactly the same Python versions in driver and worker nodes.

Probably a quick solution would be to downgrade your Python version to 3.9 (assuming driver is running on the client you're using).]


#Options to create RDD.
1) Parallelize a collection , 2) Read a file , 3) From another RDD.

Option 1: Parallelize
# Create a variable for SparkContext
sc= spark.sparkContext

# Create RDD using parallelize
numbersRDD= sc.parallelize([1,2,3,4,5])
numbersRDD.getNumPartitions()
#Get result from RDD
output=numbersRDD.collect()
print (output)
#Get any two records from RDD.
numbersRDD.take(2)
# Get first record from RDD.
# first() returns an element , while take(1) returns an RDD with one element.
numbersRDD.first()

#Create RDD with complex types.

employeesRDD=sc.parallelize( [
                             [1,'Neha',10000],
                             [2,'Steve',20000],
                              [3,'Kari',30000],
                               [4,'Ivan',40000],
                               [5,'Mohit',50000]
                               ]
                           )  

employeesRDD.first()

# Read TaxiZones.csv file and create RDD.
taxiZonesRdd=sc.textfile("\path\to\file1.csv",4)
taxiZonesRdd.getNumPartitions()
taxiZonesWithColsRdd= (taxiZonesRdd.map(lambda zone: zone.split(",")))
taxiZonesWithColsRdd.take(5)

filteredZonesRDD= (taxiZonesWithColsRdd
                         .filter(lambda zoneRow: zoneRow[1] == 'Manhattan' 
                                             and  zoneRow[2].lower().startswith("central"))
                   )
                   
filteredZonesRDD.take(5)

# Working with pair RDDs.
----------------------------
numbersRDD=sc.parallelize([2,3,4,5,6])

#Create pair RDD
-------------------
import math
numsWithSquareRootRDD=(
                           numbersRDD
                           .map(lambda num: (
                                               num, math.sqrt(num)
                                           )
                              )
                    )
numsWithSquareRootRDD.collect()

Transformation Operation -- Any function that produces a new RDD from existing RDDs. Transformation operations help in building a Lineage Graph.
Example:
--Read a file
-- Convert sales amount from INR to USD
-- Filter records with sales amount greater than 1000. 

# Transformations are lazy operations which are only executed when an Action operation is applied.

# Lineage Graph --> A lineage graph in Apache Spark is a representation of the sequence of transformations applied to 
#Resilient Distributed Datasets (RDDs).
# It's also known as the RDD lineage or the Directed Acyclic Graph (DAG) lineage.A lineage graph shows the logical execution plan of a Spark computation, 
#including the dependencies between RDDs.
#Lineage graphs are important for fault tolerance and lazy evaluation. If a partition of an RDD is lost, 
#Spark can use the lineage graph to recompute it without starting the computation from scratch.
#Each node in the graph represents an RDD, and each edge represents a transformation that leads to a new RDD.

# Transformation Operations
=========================================
Narrow Dependency Transformation & Wide Dependency Transformation.

Narrow Dependency Transformation-> A transformation where each input partition is used at-most once to produce output partitions.
Filter,Map, FlatMap, MapPartition, Sample, Union , etc. operations are examples of this.
Narrow Transformation are extremely fast . There is no data movement between partitions / no shuffling. 
Example: 

Parent RDD(RDD1)                                                                                 Child RDD (RDD2)                                                                      
-----------------                                                                                  -----------------------
Partition 1-------------------------------------------------------------------------------->      Partition1
Partition 2-------------------------------------------------------------------------------->       Partition2
Partition 3-------------------------------------------------------------------------------->       Partition3
Partition 4 ------------------------------------------------------------------------------->        Partition4            
                                Narrow
               ----------------------------------------------------->
                             Transformation


Wide Transformations & Data Shuffling. (spark.sql.shuffle.partitions)-> default 200
=============================================================
Wide Transformation is a two-step process and requires shuffling of data.Example:-> GroupBy, ReduceByKey and Distinct.
                        Step1 - Group Shuffle/Exchange                  Step2 - Sum
              TASK 1                        TASK3
Seattle | 600        Seattle|600           Seattle|600            
London  | 300        London |300           Seattle|1300    --------->    Seattle|1900
Delhi   | 700        Delhi  | 700          London|300                    London | 300
                      
              TASK2                        TASK4
Partition1          Shuffle Block1       Shuffle Partition1             Partition1

Seattle | 400       Seattle| 1300           Delhi|700
Paris   |  900      Paris  | 900            Delhi| 200     --------->     Delhi|900
Delhi   | 200       Delhi  | 200            Paris|900                     Paris|900
Seattle | 900                                                           Partition2
                   Shuffle Block2        Shuffle Partition2
Partition 2    

                  Shuffle Write    --->   Shuffle Read      ------->    Aggregate 

In wide transformation one input partition might be used multiple times to produce output partitions.
Wide transformation - 
a)Requires shuffling of data between partitions.
b) Expensive operation.
c) Shuffle Read can only start when all data is written out as shuffle blocks.

Spark Application concepts : Jobs , Stages and Tasks.
------------------------------------------------------------
Spark Applicaion is a set of resources . Contains Driver and Executor processes.

Multiple jobs can run in an application
- Number of jobs = Action operations applied.

Each job is divided into stages.
- Number of stages = Wide transformation + 1

Stages are typically executed in sequence.
- When one stage finishes , then only next can start.
- Exceptions - Join where 2 datasets can be read parallelly as separate stages.

Each stage has its own set of Tasks.
Number of Tasks = Number of Partitions
Number of parallel Tasks = Number of cores.


# Apache Spark3 Fundamentals
# 1. Performance Improvements
Adaptive Query Execution (AQE) framework
- Reoptimizes query plan at runtime based on stats.
- a) Dynamically coalescing shuffle partitions.
- b) Dynamically switching join strategy .
- c) Handling data skews in joins.


# 2. Dynamic Partition Pruning(DPP)
- Improves on Partition Pruning technique.

# 3.SQL join hints 
- Spark has multiple join strategies.
- Join hint allows to enforce a particular join strategy.
- Join hints available for each join strategy.

#Faster query compilation.
# Built-in data sources.
- New data sources like Apache Iceberg.
- Performance improvements to existing sources like Parquet , Kafka , Delta Lake , etc.

Extensibility
- Catalog API to use external catalog for managing tables (instead of Hive)

# Adaptive Query Execution: Dynamic Coalescing

Two properties should be set to true.

spark.sql.shuffle.partitions = 10   (default 200)
spark.sql.adaptive.enabled   = true  (default true)
spark.sql.adaptive.coalescePartitions.enabled = true (default true)

1. Having empty or lot of small partitions:
                --    Too many tasks are created.
                --    Reduces parallelism and consumes time/resources.

AQE dynamically coalesces shuffle partitions.
   - Removes empty partitions.
   - Combines small partitions to produce optimal sized shuffle partitions.
   
   
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


spark=(
         SparkSession
               .builder
               .appName("AqeDynamicCoalescingApp")
               
               .master ("local[4]")
               
               .config("spark.dynamicAllocation.enabled", "false")
               
               
               #Disable Adaptive Query Execution framework
               .config ("spark.sql.adaptive.enabled" , "false")
               
               .getOrCreate()
               
        )
 sc=spark.sparkContext
 
 spark

head_names=rdd_with_col.first()
df_from_rdd=rdd_with_col.toDF(head_names)
df_drop_duplicates=df_from_rdd.dropDuplicates(head_names)

df_drop_duplicates.show()


# Create method to calculate DataFrame statistics.
# Finds data for each partition.
# Calculate count of records , min & max values of a column across each partition.

def getDataFrameStats(dataframe , columnname):
      
      outputDF = (
                    dataframe 
                        .withColumn("Partition Number" , spark_partition_id()) /* spark_partition_id is a builtin function that allows you to identify the partition ID of a specific record or row within a dataframe or RDD. */
                        
                        .groupBy("Partition number")
                        .agg(
                                  count("*").alias ("Record Count"),
                                  min(columnName).alias("Min Column Value"),
                                  max(columnName).alias("Max Column Value")             
                            )      
                            
                           .orderBy("Partition Number")
                           
              )
              
              
      return outputDF
      
 # Read Yellow Taxis data
 yellowTaxisDF = (
                      spark
                         .read
                         .option("header", "true")
                         .option("inferSchema", "true")
                         .csv("C:\SparkCourse\DataFiles\Raw\YellowTaxis_202210.csv")
                 )
                 
                 
 # Check number of partitions
 print ("Partitions = " + str(yellowTaxisDF.rdd.getNumPartitions()))
 
 
 # Change Default shuffle partitions
 
 spark.conf.set("spark.sql.shuffle.partitions" , 20)
 
 
 # Apply a wide transformation
 yellowTaxiGroupedDF = (
                            yellowTaxisDF
                                .groupBy("VendorId", "payment_type")
                                .agg(sum("total_amount"))
yellowTaxiGroupedDF.show()


# Check dataframe partitions after shuffle.

# Get number of partitions
print ("Partitions = " + str (yellowTaxiGroupedDF.rdd.getNumPartitions() ))


# Get partition stats
getDataFrameStats (yellowTaxiGroupedDF , "VendorId").show() 

# Enable AQE - Dynamic Coalescing of Shuffle Partitions.

spark.conf.set ("spark.sql.adaptive.enabled" , "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")   


# Apply a wide transformation and check DataFrame stats
 yellowTaxiGroupedDF = (
                            yellowTaxisDF
                                .groupBy("VendorId", "payment_type")
                                .agg(sum("total_amount"))


# Check number of partitions.  

print ("Partitions = " + str (yellowTaxiGroupedDF.rdd.getNumPartitions() ))


# Get DataFrame Stats
getDataFrameStats(yellowTaxiGroupedDF, "VendorId").show()

# AQE : Dynamic join.

# Spark creates the execution plan before actual execution happens.

# Dynamic Switching join strategy or Manual broadcast . Which one is better ?
# When we use Dynamic switching it prevents sort operation but still it performs shuffling of data.
# But if we do manual broadcasting , it neither performs shuffling nor sorting.So manually applying broadcast is much faster but only if we are 
# certain that one of the datasets will be small after applying conditions.

# For large datasets , Shuffle Sort Merge Join is performed.
# For Broadcast Hash Join one dataset must be small.
   - # Small dataset should be less than setting spark.sql.autoBroadcastJoinThreshold
   - # Default threshold is 10 MB . So one of the dataset in the join must be smaller than 10 MB.
   
 # If highly selective filter is applied on large dataset 
 - # It may become smaller than broadcast threshold.
 - # But that's not the case . Since execution plan is ready , still performs Shuffle Sort Merge Join (if joined with large dataset)
 
 # If enabled, AQE dynamically switches from Sort Merge Join to Broadcast Hash Join at runtime.
    -  # AQE checks for dataset size after shuffle.
    
    - # If size of a dataset is now less than broadcast threshold , switches to Broadcast Hash Join.
    

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


spark=(
         SparkSession
               .builder
               .appName("AqeDynamicJoinApp")
               
               .master ("local[4]")
               
               .config("spark.dynamicAllocation.enabled", "false")
               
               
               #Disable Adaptive Query Execution framework
               .config ("spark.sql.adaptive.enabled" , "false")
               
               .getOrCreate()
               
        )
 sc=spark.sparkContext
 
 spark 


# Read Yellow Taxis file.

yellowTaxisDF = (
                      spark
                         .read
                         .option("header", "true")
                         .option("inferSchema", "true")
                         .csv("C:\SparkCourse\DataFiles\Raw\YellowTaxis_202210.csv")
                 )
                 
# Count records
yellowTaxisDF.count()

# Create two views on a large DataFrame (using the same dataframe for joining is for demo purpose only) 
# In real life scenario you would be joining two different datasets.

yellowTaxisDF.createOrReplaceTempView("YellowTaxis1")
yellowTaxisDF.createOrReplaceTempView("YellowTaxis2")

# Join two large datasets with highly selective filter on one dataset (Without enabling AQE framework)

spark.sql ("""
SELECT *
FROM YellowTaxis1 yt1
     JOIN YellowTaxis2 yt2 ON yt1.PickupLocationId = yt2.DropLocationId
     WHERE yt1.RateCodeId = 4
""" ).show()

# Enable AQE - Dynamic Switching of Join strategy 

spark.conf.set ("spark.sql.adaptive.enabled" , "true")
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "10485760b")  

# Join two large datasets with highly selective filter on one dataset (By enabling AQE framework)

spark.sql ("""

SELECT *

FROM YellowTaxis1 yt1

     JOIN YellowTaxis2 yt2 ON yt1.PickupLocationId = yt2.DropLocationId
     
     WHERE yt1.RateCodeId = 4
""" ).show()


# AQE : Handling Skew

# Data skew - When one partition has much more data than others then it's a data skew.

# In join operations:
  #    -After shuffle, data maybe unevenly distributed among partitions .
  #    - Data skew can impact query performance.
  # - Tasks processing larger partitions will take more time than ones handling smaller partitions.
  
 # If enabled , AQE dynamically optimizes data skews in joins .
     # - AQE checks for partition sizes after shuffle.
     # - Splits skewed partitions into smaller sub-partitions.
     # - Creates copy of corresponding partition on other side.
     # - Number of tasks increase , but each one will almost take same time to finish.

# It will split the skewed partition P1 into two P1-0 and P1-1 of equal size. If data is more it will split into more partitions of roughly the same 
# size.

# In the second dataframe it will create copies of corresponding partitions.


# With AQE disabled.
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


spark=(
         SparkSession
               .builder
               .appName("AqeHandlinSkewApp")
               
               .master ("local[4]")
               
               .config("spark.dynamicAllocation.enabled", "false")
               
               
               #Disable Skew Join
               .config("spark.sql.adaptive.enabled", "true")
               .config ("spark.sql.adaptive.skewJoin.enabled" , "false")
               
               .getOrCreate()
               
        )
 sc=spark.sparkContext
 
 spark 

# Create Products DataFrame - With two million unique product Ids.


productsDF = (
                   spark
                     .range(1,2000001)  # gives an ID column
                     
                     .select(
                               col("id").alias("ProductId"),
                               expr("ROUND(RAND() * 100, 2) AS PRICE")
                               
                            )
                            
           )
productsDF.show()


salesDF = (
                 spark
                     .range (1,100000001)  # gives an ID column
                     
                     .select (
                                col("id").alias("SalesId"),
                                
                                # ProductId - 70% values will be Id 1
                                
                                expr("""
                                           CASE 
                                               WHEN RAND() < 0.7 
                                                     THEN 1
                                               ELSE 
                                                   CAST (RAND() * 2000000 AS INT)
                                           END 
                                   """).alias("ProductId"),
                                
                                # Quantity - Random
                                expr ("CAST(RAND() * 10 AS INTEGER)").alias("QuantitySold")
                                
                                #Sales Date - Random
                                expr("DATE_ADD(CURRENT_DATE(), - CAST(RAND() * 365 AS INT))") .alias("SalesDate")
                             
                             )
                )

 salesDF.show()


# Create Views on Products & Sales

productsDF.createOrReplaceTempView("Products")
salesDF.createOrReplaceTempView("Sales")

#Check sales of each product.
# Data is highly skewed in favor of ProductId=1

spark.sql("""
SELECT ProductId , COUNT(*) AS ProductCount

FROM Sales

GROUP BY  ProductId

ORDER BY ProductCount DESC

""").show()


# Find total number of products sold per day - With AQE : Skew join disabled.

sparl.sql("""

SELECT s.SalesDate , SUM(Price * QuantitySold) AS SalesAmount

FROM Sales s

        JOIN Products p ON p.ProductId = s.ProductId
        
GROUP BY s.salesDate

ORDER BY SalesAmount DESC 

""").show()


# Enable Adaptive Query Execution - Handling Data Skew in Joins

spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

                                
sparl.sql("""

SELECT s.SalesDate , SUM(Price * QuantitySold) AS SalesAmount

FROM Sales s

        JOIN Products p ON p.ProductId = s.ProductId
        
GROUP BY s.salesDate

ORDER BY SalesAmount DESC 

""").show()



# Dynamic Partition Pruning - In Spark is a query optimization technique that improves performance by dynamically determining 
#which partitions need to be scanned during query execution, based on runtime information, particularly in join scenarios, 
#thus reducing the amount of data read and processed. 

#  - DPP typically involves a smaller dimension table being joined with a larger fact table. 
#  - The filter condition from the dimension table is used to create a dynamic filter that is broadcasted to all executors. 
#  - At runtime, Spark uses this dynamic filter to determine which partitions of the fact table need to be scanned, 
     #effectively skipping irrelevant partitions. 
     
     
#BENEFITS - 
 #  - Improved query performance: By scanning only necessary partitions, DPP significantly reduces I/O and processing time, 
 #  especially for large datasets. 
#  - Resource efficiency: DPP helps to optimize resource utilization by minimizing the amount of data that needs to be read and processed. 

# Use cases -

 #Star schema queries: DPP is particularly effective for queries involving large fact tables joined with smaller dimension tables, 
 #a common pattern in data warehouses. 
 
 #Join queries with dynamic filters: DPP excels when filtering conditions are based on the results of a join or other runtime information. 

SELECT /*+ BROADCASTJOIN */ *

FROM Sales s
   JOIN Products p
   ON s.ProductId = p.ProductId
   
WHERE s.ProductId = 3

#Conditions -
Dynamic Partition Pruning must be enabled.
   - spark.sql.optimizer.dynamicPartitionPruning.enabled

Large tables must have disk partitions.
During join, small table should be broadcasted.
                             
                   
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


spark=(
         SparkSession
               .builder
               .appName("DynamicPartitionPruningApp")
               
               .master ("local[4]")
               
               .config("spark.dynamicAllocation.enabled", "false")
               .config("spark.sql.adaptive.enabled", "false")
               
               #Disable Dynamic Partition Pruning
               
               .config ("spark.sql.optimizer.dynamicPartitionPruning.enabled" , "false")
               
               .getOrCreate()
               
        )
 sc=spark.sparkContext
 
 spark     


# Read Yellow Taxis file.
 
yellowTaxisDF = (
                      spark
                         .read
                         .option("header", "true")
                         .schema(yellowTaxiSchema)
                         .csv("C:\SparkCourse\DataFiles\Raw\YellowTaxis_202210.csv")
                 )                      
                 
# Save Yellow Taxis as a partitioned table.

(

     yellowTaxisDF
            .write
            
            .partitionBy("PickupLocationId")

            .option("header", "true")
            .option("dateFormat", "yyyy-MM-dd HH:mm:ss.S")
            .mode ("overwrite")
            .format("csv")
            .option("path", "C:\SparkCourse\DataFiles\Output\YellowTaxisPartitioned.csv")
            
            .saveAsTable("YellowTaxis")
 )
 
 # Save small DataFrame as a non-partitioned table
taxiZoneSchema = (
                    spark
                      .read                    
                      .schema(taxiZoneSchema)
                      .csv("C:\SparkCourse\DataFiles\Raw\TaxiZones.csv")
                )

# Save Taxi Zones as a non-partitioned table
(
      taxiZonesDF
            .write
            .option("header", "true")
            .option("dateFormat", "yyyy-MM-dd HH:mm:ss.S")            
            .mode ("overwrite")
            .format("csv")
            .option("path", "C:\SparkCourse\DataFiles\Output\TaxiZones.csv")
            
            .saveAsTable("TaxiZones")
 )
 
 
 # Join Yellow Taxis and Taxi Zones with filter on Yellow Taxis table 
   #  - Filtering on same column (PickupLocationId) by which data is partitioned.
   
 spark.sql("""
 
 SELECT *
 
 FROM YellowTaxis yt
       JOIN TaxiZones tz ON yt.PickupLocationId = tz.PickupLocationId
       
       WHERE yt.PickupLocationId = 1
       
""").show()

# Check Pickup Location Ids in one Borough

# 'EWR' borough has only one Pickup Location Id , which is Id=1

sparl.sql ("""

SELECT *

FROM TaxiZones

WHERE Borough = 'EWR'

""").show()


# DPP disabled: Join Yellow Taxis and Taxi Zones with filter on TaxiZones table.

#Output is the same as the previous query.
#But since filter is on TaxiZones, Partition Pruning will not work.

 spark.sql("""
 
 SELECT *
 
 FROM YellowTaxis yt
 
       JOIN TaxiZones tz ON yt.PickupLocationId = tz.PickupLocationId
       
       WHERE tz.Borough = 'EWR' -- WHERE PickupLocationId = 1 (Both will yield same output)
       
       
""").show()

# Enable Dynamic Partition Pruning

spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")


# DPP enabled : Join Yellow Taxis and Taxi Zones with filter on TaxiZones table.

# Partition Pruning will work.

spark.sql("""
 
 SELECT *
 
 FROM YellowTaxis yt
 
       JOIN TaxiZones tz ON yt.PickupLocationId = tz.PickupLocationId
       
       WHERE tz.Borough = 'EWR' 
       
       
""").show()


