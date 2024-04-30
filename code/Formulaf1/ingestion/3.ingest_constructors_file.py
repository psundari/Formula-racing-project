# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the Constructor File**

# COMMAND ----------

from pyspark.sql.types import StructField,StringType,IntegerType,StructType

constructor_schema=StructType([StructField('constructorId',IntegerType(),False),
                               StructField('constructorRef',StringType(),True),
                               StructField('name',StringType(),True),
                               StructField('nationality',StringType(),True)])

# COMMAND ----------

constructor_df=spark.read.format("csv")\
    .option('header',True)\
    .schema(constructor_schema)\
    .load(f'{raw_folder_path}/constructors.csv')

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

from pyspark.sql.functions import col,lit

# COMMAND ----------

constructor_with_timestamp=add_ingestion_date(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Select the required columns & Rename them**

# COMMAND ----------

constructor_selected_df=constructor_with_timestamp.withColumn("data_source", lit(v_data_source)).select(col('constructorId').alias('constructor_id'),col('constructorRef').alias('constructor_ref'),col('name'),col('nationality'),col('ingestion_date'),col("data_source"))


# COMMAND ----------

display(constructor_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Write to processed container**

# COMMAND ----------

constructor_selected_df.write.mode('overwrite').parquet(f'{processed_folder_path}/constructor')

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

display(spark.read.parquet(f'{processed_folder_path}/constructor'))

# COMMAND ----------

