DataFrames are built on top of RDDs . All features of RDDs are applied to DataFrames.

Data Quality Checks -
- Completeness -  Remove null or missing data 
               -  Fill missing values with placeholder.
- Uniqueness   - Remove duplicate records.

- Timeliness   - Appropriate data range

- Accuracy     - Remove inaccurate data 

Spark has a great feature to analyze numerical columns in a dataframe.
yellowTaxiAnalyzeDF= ( 
                           yellowTaxiDF.describe
                            (
                                "passenger_count",
                                "trip_distance"
                            )    

                    ) 

yellowTaxiAnalyzeDF.show()
# Display count before operation
print ("Before operation= " + str( yellowTaxiDF.count()) )

yellowTaxiDF=   ( 
                    yellowTaxiDF.where("passenger_count > 0").filter(col("trip_distance") > 0.0)
                                         

                )
                
#Display the count after operation
print ("After operation= " + str( yellowTaxiDF.count()) )


from pyspark.sql import SparkSession
from datetime import datetime, date
spark=SparkSession.builder.appName("Tranformations.com").getOrCreate()

df=spark.createDataFrame([(1,101,1001,2,30,date(2023,1,1)),
                          (1,101,1001,2,30,date(2023,1,1)),
 (2,102,1002,1,20,date(2023,2,1)),
 (3,103,1003,5,15,date(2023,3,1)),(4,104,1004,3,30,date(2023,4,1)),
 (5,105,1005,2,20,date(2023,5,1)),
  (5,105,1005,2,20,date(2023,5,1))                       
 ],schema=["TransactionID","CustomerID","ProductID","Quantity","Price","Date"] )
 
 # DROP DUPLICATES
 -------------------------
 
 df.show()
 
 df.dropDuplicates().show()
 
 # RENAME
 ---------------------------------
 
 #Rename columns and amend NULLs
A) df2=df2.withColumnRenamed("_c0","ID")\
    .withColumnRenamed("Ball control","Ball_Control")\
    .withColumnRenamed("Sliding tackle", "Sliding_Tackle")
    
B) df=df.withColumnRenamed("TransactionID","Transaction_ID")\
     .withColumnRenamed("CustomerID","Customer_ID")\
     .withColumnRenamed("ProductID","Product_ID")
df.show()

C) from pyspark.sql import Row
df = spark.createDataFrame([
    Row(name='Alice', age=5, height=80),
    Row(name='Alice', age=5, height=80),
    Row(name='Alice', age=10, height=80)
])

df.dropDuplicates().show()

#ADDING CALCULATED COLUMNS
-------------------------------

df=spark.createDataFrame([(1,101,1001,2,30,date(2023,1,1)),
                          (1,101,1001,2,30,date(2023,1,1)),
 (2,102,1002,1,20,date(2023,2,1)),
 (3,103,1003,5,15,date(2023,3,1)),(4,104,1004,3,30,date(2023,4,1)),
 (5,105,1005,2,20,date(2023,5,1)),
  (5,105,1005,2,20,date(2023,5,1))
 ],schema=["TransactionID","CustomerID","ProductID","Quantity","Price","Date"] )
 
 df.show()
 
 from pyspark.sql.functions import col, upper
df_with_sales=df.withColumn("Total Sales", col("Quantity") * col("Price"))
df_with_sales.show()

# INCREASE / DECREASE PARTITIONS
----------------------------------------
# How many partitions in DF. By default 2 partitions.
df.rdd.getNumPartitions()

# Repartition -  Increase/ Decrease the partitions in Df
df2=df.repartition(4)
df2.rdd.getNumPartitions()

# To reduce number of partitions
df3=df2.coalesce(1)
df2.show()
df3.rdd.getNumPartitions()


#DEALING WITH NULL VALUES 
----------------------------

#Strategy 1: Filtering Nulls — Cleaning Your Data

#When dealing with null values, the first step is often to filter them out.
 #This ensures that you’re working with clean and meaningful data.
 
 null_sample_dataset=spark.createDataFrame([(1 ,"John" , 25 , 50000),
                                           (2 ,"Alice" , None , 60000),
                                           (3 ,"Bob" , 30 , None),
                                           (4 ,"Carol" , 28 , 55000),
                                           (5, "David", None, 48000)],schema = ["ID", "Name", "Age", "Salary"])
                                           
df_filtered = null_sample_dataset.filter(null_sample_dataset["Age"].isNotNull())
df_filtered.show()

#Strategy 2: Replacing Nulls — Filling the Gaps

#In some cases, replacing nulls with specific values makes sense. This prevents missing values from affecting your analyses.

df_filled = null_sample_dataset.na.fill({"Age": 0, "Salary": 0})
df_filled.show()

#Strategy 3: Using Coalesce — Choosing Non-Null Values
#The `coalesce()` function helps you select the first non-null value from a list of columns.
#The COALESCE function returns the first non-NULL value from a series of expressions. 
#The expressions are evaluated in the order in which they are specified, and the result of the function is the first value that is not null.

from pyspark.sql.functions import coalesce
df_coalesced = null_sample_dataset.withColumn("Age", coalesce(null_sample_dataset["Age"]))
df_coalesced.show()

#Strategy 4: Aggregating with Nulls — Calculation Confidence

#Aggregation functions like `avg()`, `sum()`, etc., automatically ignore null values when computing results.

from pyspark.sql.functions import avg

avg_salary = null_sample_dataset.select(avg(null_sample_dataset["Salary"])).collect()[0][0]
print("Average Salary:", avg_salary)

#Window functions like `lead()` and `lag()` can work gracefully with nulls, offering insights into sequential data.


df_with_lead = null_sample_dataset.withColumn("Next_Age", lead(null_sample_dataset["Age"]).over(window_spec))
df_with_lead.show()

#How to deal with nulls in Apache Spark?
# DataFrameNAFunctions class is used to work with missing data .Accessible using na keyword.
#Dealing with null values in Apache Spark within DataFrames is crucial for data cleaning and preparation.
# Null values can be managed effectively using various methods provided by Spark's DataFrame API. Here are some common techniques to handle nulls:

#1. 'all' - Remove rows with null values in all columns.
#   'any' - Remove rows with null values in any column.

# Display count before operation
print ("Before operation= " + str( yellowTaxiDF.count()) )

yellowTaxiDF=   ( 
                    yellowTaxiDF.na.drop('all')
                                         

                )

# Display count after operation
print ("After operation= " + str( yellowTaxiDF.count()) )

#2. Completeness Check : Replace nulls with default values.

defaultValueMap= {'payment_type':5 , 'RateCodeID': 1}

yellowTaxiDF= (
                  yellowTaxiDF.na.fill(defaultValueMap)
              )
              
#3. Uniqueness Check: Drop duplicate rows
# Display count before operation
print ("Before operation= " + str( yellowTaxiDF.count()) )

yellowTaxiDF= (
                  yellowTaxiDF.dropDuplicates()
              )


# Display count after operation
print ("After operation= " + str( yellowTaxiDF.count()) )

#4. Timeliness check : Remove records outside the bound.
# Display count before operation
print ("Before operation= " + str( yellowTaxiDF.count()) )

yellowTaxiDF= (
                  yellowTaxiDF.where("lpep_pickup_datetime >= '2022-10-01' AND lpep_dropoff_datetime < '2022-11-01' ")
              )


# Display count after operation
print ("After operation= " + str( yellowTaxiDF.count()) )

# Cleanup data by applying Data Quality checks . (chain all the above operations)

defaultValueMap={'payment_type': 5, 'RateCodeID': 1}

# Read File
yellowTaxiDF= (
                 spark
                 .read
                 .option("header", "true")
                 .schema (yellowTaxiSchema)
                 .csv ("C:\SparkCourse\DataFiles\Raw\YellowTaxis_202210.csv")
              )
              
# Cleanup data by applying data quality checks.
yellowTaxiDF=(
                   yellowTaxiDF
                   .where ("passenger_count > 0")
                   .filter(col("trip_distance") > 0.0)
                   .na.drop('all')
                   .na.fill(defaultValueMap)
                   .dropDuplicates()
                   .where("lpep_pickup_datetime >= '2022-10-01' AND lpep_dropoff_datetime < '2022-11-01' ")
             )            

#5. Identification of Null Values: 
 - Use functions like ‘isNull()’ or ‘isNotNull()’ to spot null values in DataFrame columns.

#6. Using ‘dropMalformed’:
 - Employ ‘dropMalformed’ mode to discard rows that don't conform to the defined schema datatypes.

#7. Dropping Null Values: 
 - Utilize the ‘dropna’ method to eliminate rows containing null or NaN values.
 df.dropna()
 df.dropna(subset=['column1', 'column2'])

#8. Filling Null Values: 
 - Leverage the ‘fillna’ method to substitute null values with specified constants or computed values.
 df.fillna(value=0)
 df.fillna(value={'column1': 0, 'column2': 'unknown'})

#9. Coalesce Function: 
 - Use the ‘coalesce’ function to select the first non-null value from a list of columns.
 df.withColumn("new_column", coalesce("column1", "column2"))

 -Replacing null values with the constant. 
 df.withColumn("new_column", coalesce("column1", lit(0)))

# DROP ROWS WITH NULLS
------------------------------------------
dropped_nulls=null_sample_dataset.dropna()
dropped_nulls.show()


# Applying Tranformations
======================================
yellowTaxiDF.printSchema()

# Select limited columns.

yellowTaxiDF=(
                yellowTaxiDF
                     
                     # Select only Limited Columns
                     .select(
                                "VendorID",
                                col("passenger_count").cast(IntegerType()),
                                column("trip_distance").alias("TripDistance"),
                                yellowTaxiDF.lpep_pickup_datetime,
                                "lpep_dropoff_datetime",
                                "PUlocationID",
                                "DOlocationID",
                                "RateCodeID",
                                "total_amount",
                                "payment_type"
                             )
                       #Don't run, since airport_fee has not been selected above 
                       # .drop("airport_fee")                       
         )
yellowTaxiDF.printSchema()

# Rename columns
====================================================

yellowTaxiDF=( 
                   yellowTaxiDF
                   .withColumnRenamed("passenger_count" , "PassengerCount")
                   .withColumnRenamed("lpep_pickup_datetime", "PickupTime")
                   .withColumnRenamed("lpep_dropoff_datetime", "DropTime")
                   .withColumnRenamed("PUlocationID", "PickupLocationId")
                   .withColumnRenamed("DOlocationID", "DropLocationId")
                   .withColumnRenamed("total_amount", "TotalAmount")
                   .withColumnRenamed("payment_type", "PaymentType")
            )
            
yellowTaxiDF.printSchema()



                   

           )
         
                                
#Create derived columns - TripYear , TripMonth , TripDay
=========================================================================

yellowTaxiDF= (
                        yellowTaxiDF
                        
                        .withColumn("TripYear" , year(col("PickupTime"))
                        
                        .select (
                                   "*",
                                   expr ("month(PickupTime) as TripMonth"),
                                   dayofmonth(col("PickupTime")).alias("TripDay")
                                )
              )
              
              
yellowTaxiDF.printSchema()

# Create derived column - TripTimeInMinutes

# Option1
yellowTaxiDF= (
                      yellowTaxiDF
                      .withColumn("TripTimeInMinutes",
                                                round (
                                                         (unix_timestamp(col("DropTime"))
                                                               - unix_timestamp(col("PickupTime")))
                                                               
                                                            /60
                                                      )
                                 )
            )
            
yellowTaxiDF.printSchema()


#option 2 
tripTimeInSecondsExpr= unix_timestamp(col("DropTime")) - unix_timestamp(col("PickupTime"))

tripTimeInMinutesExpr= round(tripTimeInSecondsExpr / 60 )

yellowTaxiDF=(
                  yellowTaxiDF
                        .withColumn("TripTimeInMinutes" , tripTimeInMinutesExpr)
             )
             
yellowTaxiDF.printSchema()

# Create derived column - TripType

tripTypeColumn= (
                    when (
                             col("RatecodeID") == 6,
                                "SharedTrip"
                        )
                        
                   .otherwise("SoloTrip")
                )

yellowTaxiDF=(
                  yellowTaxiDF
                  
                          .withColumn("TripType", tripTypeColumn)
                          
                          .drop("RatecodeID")
                          
           )
yellowTaxiDF.printSchema()

# Check Execution plans
==================================

yellowTaxiDF.explain ( mode= "extended" )

# Other modes - simple, codegen , cost , formatted 


# Extract nested fields from JSON
=========================================

taxiBasesFlatDF=(  
                     taxiBasesDf
                        .select (
                                  col("License Number").alias ("BaseLicenseNumber"),
                                  col("Entity Name").alias ("EntityName"),
                                  
                                  col("Address.Building").alias("AddressBuilding"),
                                  
                                  col("Address.Street").alias("AddressStreet"),
                                  col("Address.City").alias("AddressCity"),
                                  col("Address.State").alias("AddressState"),
                                  col("Address.PostCode").alias("AddressPostCode"),
                                  
                                  col("Geolocation.Latitude").alias ("GeoLatitude"),
                                  col("Geolocation.Longitude").alias("GeoLongitude")
                               )
                )

taxiBasesFlatDF.show()                

# Aggregate Data

yellowTaxiDFReport = (
                          yellowTaxiDF
                          
                              .groupBy("PickupLocationId", "DropLocationId")
                              .agg (
                                      avg("TripTimeInMinutes").alias("AvgTripTime"),
                                      sum("TotalAmount").alias("SumAmount")
                                   )
                                   
                               .orderBy(col ("PickupLocationId").desc())
                  )
                  
yellowTaxiDFReport.show()
                                  
# Spark Functions -
# Date & Time
- date_add , date_format , next_day , to_date
- current_timestamp , to_timestamp

# Mathametical
- round , ceil , floor , log , sqrt

#String
- lower, upper, length , substring, trim ,split

# Array
- concat , array_contains , array_join, filter, explode

# Others
- when , least , greatest , isnull

# Handling Corrupt Data 
=======================================================
#In PySpark, when reading data into a DataFrame from external sources, you can specify a reading mode to control how the system should handle 
#issues such as missing files, corrupt records, and schema mismatches.
 #The available reading modes depend on the data source.
 
 # File_Error handling or Reading modes in PySpark.
 #1.  PERMISSIVE MODE - JSON and CSV:
 
 #In permissive mode, PySpark reads as much data as possible and stores corrupt records in a “_corrupt_record” column.
 # Define the manually schema also.(keywords - StructType , StructField classes .
 
 from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# A.Define the schema for the DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
schema = StructType([
    StructField("EmployeeID", IntegerType(), True),
    StructField("EmployeeName", StringType(), True),
    StructField("Address", StringType(), True),
    StructField("_corrupt_record", StringType(), True)
])
We have to add _corrupt_record into a predefined schema. so that we can get corrupted record details under this column.

#B.Read CSV File which contains corrupted record.
employee_df = spark.read.option("mode", "PERMISSIVE")
    .schema(schema)
    .option("header", True)
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .csv("./employee.csv").cache()

We have to choose the mode as PERMISSIVE type
We have to add the columnNameOfCorruptRecord option as column name which we have given in schema.
In the output data frame, we got corrupted records in a separate column.

#C. Filter the cleaned records
from pyspark.sql.functions import col
employee_df.where(col("_corrupt_record").isNull()).drop("_corrupt_record")

By Filtering the null value of the _corrupt_record column we can get cleaned data or proper data. so that we can proceed with further processing.

#D. Filter Corrupted records
from pyspark.sql.functions import col
employee_df.select(col("_corrupt_record")).where(col("_corrupt_record").isNotNull())

#By Filtering not null value of _corrupt_record column we can get corrupted data separately. so that we can store/write these corrupted or bad records 
#into a separate folder and initiate a notification to the developer or respective person.

Text formats (CSV and JSON) have built-in support for handling corrupt records.

# Read JSON file with default Parse Mode
----------------------------------------------

rateCodesJsonDF_modes= (
                            spark
                            .read
                            .json("C:\SparkCourse\DataFiles\Raw\RateCodes.json")
                      )
                      
rateCodesJsonDF_modes.show(truncate=False)

# Read JSON file with Permissive mode
--------------------------------------------
rateCodesJsonDF_modes= (
                            spark
                            .read
                            .option("mode", "PERMISSIVE")
                            .json("C:\SparkCourse\DataFiles\Raw\RateCodes.json")
                      )
                      
rateCodesJsonDF_modes.show(truncate=False)


#2. DROPMALFORMED :
This mode drops any row that contains malformed data (e.g., extra columns).

# Read JSON file with DropMalformed mode
--------------------------------------------
rateCodesJsonDF_modes= (
                            spark
                            .read
                            .option("mode", "DROPMALFORMED")
                            .json("C:\SparkCourse\DataFiles\Raw\RateCodes.json")
                      )
rateCodesJsonDF_modes.show(truncate=False)

#3. FailFast Mode (FAILFAST) - JSON and CSV:
This mode fails the reading process if it encounters any malformed data or schema mismatch.

# Read JSON file with FailFast mode
-------------------------------------------
rateCodesJsonDF_modes=(
                           spark
                            .read
                            .option("mode", "FAILFAST")
                            .json("C:\SparkCourse\DataFiles\Raw\RateCodes.json")
                      )
rateCodesJsonDF_modes.show(truncate=False)

# Read CSV file with default Permissive mode
----------------------------------------------
rateCodesCsvDF_modes= (
                            spark
                            .read
                            .option("header", "true")
                            .option("mode", "PERMISSIVE")
                            .csv("C:\SparkCourse\DataFiles\Raw\RateCodes.csv")
                      )
                      
rateCodesCsvDF_modes.show(truncate=False)


 





             

                                   