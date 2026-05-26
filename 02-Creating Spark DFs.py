# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.option("header",True).csv('/FileStore/tables/StudentData.csv')
df.show()

# Read option 1
vehicles_df = spark.read.option("header" , True).option("inferSchema" , True).csv("/Volumes/my_catalog/new_schema/my_volume/sample_data.csv")
vehicles_df.show()

#Read option 2
vehicles_df = spark.read.options(header = True, inferSchema = True).csv("/Volumes/my_catalog/new_schema/my_volume/sample_data.csv")

vehicles_df.show()


#Read option 3

df = spark.read.format("csv").option("header", True).load("/Volumes/my_catalog/new_schema/my_volume/sample_data.csv")

df.show()


vehicles_df.printSchema()
