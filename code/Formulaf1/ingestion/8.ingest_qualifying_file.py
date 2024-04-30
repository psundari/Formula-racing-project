# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC **Read Qualifying data from raw container**

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,StringType,IntegerType,DateType

# COMMAND ----------

qua_schema=StructType([StructField('qualifyId',IntegerType(),False),
                       StructField('raceId',IntegerType(),True),
                       StructField('driverId',IntegerType(),True),
                       StructField('ConstructorId',IntegerType(),True),
                       StructField('number',IntegerType(),True),
                       StructField('position',IntegerType(),True),
                       StructField('q1',StringType(),True),
                       StructField('q2',StringType(),True),
                       StructField('q3',StringType(),True)])

# COMMAND ----------

qualifying_df=spark.read.format("csv")\
    .schema(qua_schema)\
    .option('header','True')\
    .load(f"{raw_folder_path}/qualifying.csv")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

qualifying_columns_df=qualifying_df.withColumnRenamed('qualifyId','qualify_id')\
    .withColumnRenamed('raceId','race_id')\
    .withColumnRenamed('driverId','driver_id')\
    .withColumnRenamed('constructorId','constructor_id')\
    .withColumn("data_source", lit(v_data_source))
qualifying_with_timestamp=add_ingestion_date(qualifying_columns_df)

# COMMAND ----------

display(qualifying_with_timestamp)

# COMMAND ----------

# MAGIC %md
# MAGIC **Write data to processed Container**
# MAGIC

# COMMAND ----------

qualifying_with_timestamp.write.mode('overwrite').parquet(f"{processed_folder_path}/qualifying")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/qualifying"))

# COMMAND ----------

