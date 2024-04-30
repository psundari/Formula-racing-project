# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType

# COMMAND ----------



circuits_schema=StructType([StructField("circuitId",IntegerType(),False),
                                   StructField("circuitRef",StringType(),True),
                                   StructField("name",StringType(),True),
                                   StructField("location",StringType(),True),
                                   StructField("country",StringType(),True),
                                   StructField("lat",DoubleType(),True),
                                   StructField("lng",DoubleType(),True),
                                   StructField("alt",IntegerType(),True),
                                   StructField("url",StringType(),True)
                                   ])

# COMMAND ----------

circuits_df=spark.read.format("csv")\
    .option('header','True')\
    .schema(circuits_schema)\
    .load(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Select only required columns

# COMMAND ----------

from pyspark.sql.functions import col,lit
circuits_datasource=circuits_df.withColumn("data_source", lit(v_data_source))
circuits_selected_df=circuits_datasource.select(col("circuitId").alias("circuit_id"),col("circuitRef").alias("circuit_ref"),
                                        col("name"),col("location"),col("country"),col("lat").alias("latitude"),
                                        col("lng").alias("longitude"),col("alt").alias("altitude"),col("data_source"))
#following are the other ways to select required columns
# circuits_selected_df=circuits_df.select("circuitId","circuitRef","name","location","country","lat","lng","alt")
# circuits_selected_df=circuits_df.select(circuits_df.circuitId,circuits_df.circuitRef)

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Adding new column(ingestion date)

# COMMAND ----------

circuits_with_timestamp=add_ingestion_date(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write data to datalake as parquet

# COMMAND ----------


circuits_with_timestamp.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/circuits"))

# COMMAND ----------

