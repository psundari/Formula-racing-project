# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC **Read the Laptimes file from raw container**

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,StringType,IntegerType,DateType

# COMMAND ----------

lap_times_schema=StructType([StructField('raceId',IntegerType(),False),
                             StructField('driverId',IntegerType(),True),
                             StructField('lap',StringType(),True),
                             StructField('position',IntegerType(),True),
                             StructField('time',StringType(),True),
                             StructField('milliseconds',IntegerType(),True)])

# COMMAND ----------

lap_times_df=spark.read.format("csv")\
    .option('header','True')\
    .schema(lap_times_schema)\
    .load(f"{raw_folder_path}/lap_times.csv")

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

lap_times_columns_df=lap_times_df.withColumnRenamed('raceId','race_id')\
    .withColumnRenamed('driverId','driver_id')\
    .withColumn("data_source", lit(v_data_source))
lap_with_timestamp=add_ingestion_date(lap_times_columns_df)

# COMMAND ----------

display(lap_with_timestamp)

# COMMAND ----------

# MAGIC %md
# MAGIC **Write file to processed container**

# COMMAND ----------

lap_with_timestamp.write.mode('overwrite').parquet(f'{processed_folder_path}/laptimes')

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

display(spark.read.parquet(f'{processed_folder_path}/laptimes'))

# COMMAND ----------

