# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC **Read the data from raw container**

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,StringType,IntegerType,DateType

# COMMAND ----------

driver_schema=StructType([StructField("driverId",IntegerType(),False),
                          StructField("driverRef",StringType(),True),
                          StructField("number",IntegerType(),True),
                          StructField("code",StringType(),True),
                          StructField("forename",StringType(),True),
                          StructField("surname",StringType(),True),
                          StructField("dob",DateType(),True),
                          StructField("nationality",StringType(),True),
                          StructField("url",StringType(),True)
                                   ])

# COMMAND ----------

driver_df=spark.read.format("csv")\
    .option('header','True')\
    .schema(driver_schema)\
    .load(f"{raw_folder_path}/drivers.csv")
display(driver_df)

# COMMAND ----------

from pyspark.sql.functions import col,concat,lit

# COMMAND ----------

driver_with_timestamp=add_ingestion_date(driver_df)
driver_concat=driver_with_timestamp.withColumn('name',concat(col('forename'),lit(' '),col('surname')))

# COMMAND ----------

display(driver_concat)

# COMMAND ----------

driver_selected_df=driver_concat.withColumn("data_source", lit(v_data_source)).select(col('driverId').alias('driver_id'),col('driverRef').alias('driver_ref'),col('number'),col('code'),col('name'),col('dob'),col('nationality'),col('ingestion_date'),col("data_source"))

# COMMAND ----------

display(driver_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Write to processed container**

# COMMAND ----------

driver_selected_df.write.mode('overwrite').parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/drivers"))

# COMMAND ----------

