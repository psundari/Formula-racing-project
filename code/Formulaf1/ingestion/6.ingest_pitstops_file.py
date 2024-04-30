# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC **Read Pitstops csv file from raw container**

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,StringType,DateType,IntegerType

# COMMAND ----------

pitstops_schema=StructType([StructField('raceId',IntegerType(),False),
                            StructField('driverId',IntegerType(),True),
                            StructField('stop',StringType(),True),
                            StructField('lap',IntegerType(),True),
                            StructField('time',StringType(),True),
                            StructField('duration',StringType(),True),
                            StructField('milliseconds',IntegerType(),True)])

# COMMAND ----------

pit_stops_df=spark.read.format("csv")\
    .option('header','True')\
    .schema(pitstops_schema)\
    .load(f"{raw_folder_path}/pit_stops.csv")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
pit_stops_columns_df=pit_stops_df.withColumnRenamed('raceId','race_id')\
    .withColumnRenamed('driverId','driver_id')\
    .withColumn("data_source", lit(v_data_source))
pitstops_with_timestamp=add_ingestion_date(pit_stops_columns_df)

# COMMAND ----------

display(pitstops_with_timestamp)

# COMMAND ----------

# MAGIC %md
# MAGIC **Write file to processed Container**
# MAGIC

# COMMAND ----------

pitstops_with_timestamp.write.mode("overwrite").parquet(f'{processed_folder_path}/pitstops')

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

display(spark.read.parquet(f'{processed_folder_path}/pitstops'))

# COMMAND ----------

