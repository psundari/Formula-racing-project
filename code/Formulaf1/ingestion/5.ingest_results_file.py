# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC **Read Results File from Raw container**

# COMMAND ----------

from pyspark.sql.types import StringType,IntegerType,FloatType,StructField,StructType

# COMMAND ----------

results_schema=StructType([StructField('resultId',IntegerType(),False),
                           StructField('raceId',IntegerType(),True),
                           StructField('driverId',IntegerType(),True),
                           StructField('constructorId',IntegerType(),True),
                           StructField('number',IntegerType(),True),
                           StructField('grid',IntegerType(),True),
                           StructField('position',IntegerType(),True),
                           StructField('positionText',StringType(),True),
                           StructField('positionOrder',IntegerType(),True),
                           StructField('points',IntegerType(),True),
                           StructField('laps',IntegerType(),True),
                           StructField('time',StringType(),True),
                           StructField('milliseconds',IntegerType(),True),
                           StructField('fastestLap',IntegerType(),True),
                           StructField('rank',IntegerType(),True),
                           StructField('fastestLapTime',StringType(),True),
                           StructField('fastestLapSpeed',FloatType(),True),
                           StructField('statusId',StringType(),True)])

# COMMAND ----------

results_df=spark.read.format("csv")\
    .option('header','True')\
    .schema(results_schema)\
    .load(f"{raw_folder_path}/results.csv")

# COMMAND ----------

display(results_df)

# COMMAND ----------

from pyspark.sql.functions import col,lit

# COMMAND ----------

results_with_columns_df=results_df.withColumnRenamed('resultId','result_id')\
    .withColumnRenamed('raceId','race_id')\
    .withColumnRenamed('driverId','driver_id')\
    .withColumnRenamed('constructorId','constructor_id')\
    .withColumnRenamed('positionText','position_text')\
    .withColumnRenamed('positionOrder','position_order')\
    .withColumnRenamed('fastestLap','fastest_lap')\
    .withColumnRenamed('fastestLapTime','fastest_lap_time')\
    .withColumnRenamed('fastestLapSpeed','fastest_lap_speed')\
    .withColumn("data_source", lit(v_data_source))
results_with_timestamp=add_ingestion_date(results_with_columns_df)

# COMMAND ----------

results_final_df=results_with_timestamp.drop(col('statusId'))

# COMMAND ----------

# MAGIC %md
# MAGIC **Write data to processed Container**

# COMMAND ----------

results_final_df.write.mode('overwrite').partitionBy('race_id').parquet(f'{processed_folder_path}/results')

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

display(spark.read.parquet(f'{processed_folder_path}/results'))

# COMMAND ----------

