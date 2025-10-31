from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder.appName("ExclusiveGoldWinners").getOrCreate()

data = [
    (1, '100m', 2023, 'Raj', 'John', 'DSouza'),
    (2, '200m', 2023, 'Paul', 'Harry', 'Raj'),
    (3, '300m', 2023, 'John', 'Barry', 'Vinod'),
    (4, '500m', 2023, 'Yuvi', 'Saikat', 'Narender'),
    (5, '200m', 2023, 'Chandra', 'Santosh', 'Raghav'),
    (6, '300m', 2023, 'Yuvi', 'Charles', 'Thomas')
]

columns = ["Serial_No", "Event", "Year", "GOLD", "SILVER", "BRONZE"]
df = spark.createDataFrame(data, columns)

# 1) distinct names in GOLD as "name"
gold_df = df.select(col("GOLD").alias("name")).distinct()

#gold_df.show()

#2) distinct names who WON non-gold (SILVER or BRONZE) as "name"
non_gold_df = (
    df.select(col("SILVER").alias("name"))
      .union(df.select(col("BRONZE").alias("name")))
      .distinct()
)

#non_gold_df.show()

# # 3) people who are in GOLD but NOT in SILVER/BRONZE (left_anti)
exclusive_gold = gold_df.join(non_gold_df, on="name", how="left_anti")

# # 4) count gold medals only for those exclusive gold winners
# # join back to original df by comparing df.GOLD to exclusive_gold.name (no ambiguous column names)
result_df = (
    df.join(exclusive_gold, df["GOLD"] == exclusive_gold["name"], how="inner")
      .groupBy(exclusive_gold["name"].alias("name"))
      .agg(count("*").alias("number_of_gold_medals"))
)

result_df.show(truncate=False)
