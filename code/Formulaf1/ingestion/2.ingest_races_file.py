# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##DATA INGESTION:

# COMMAND ----------

# MAGIC %md 
# MAGIC **Races File**

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType
races_schema=(StructType([StructField("raceId",IntegerType(),False),
                          StructField("year",IntegerType(),True),
                          StructField("round",IntegerType(),True),
                          StructField("circuitId",IntegerType(),True),
                          StructField("name",StringType(),True),
                          StructField("date",DateType(),True),
                          StructField("time",StringType(),True),
                          StructField("url",StringType(),True)]))

# COMMAND ----------

races_df=spark.read.format("csv")\
    .option("header",True)\
    .schema(races_schema)\
    .load(f"{raw_folder_path}/races.csv")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,to_timestamp,concat,col,lit

# COMMAND ----------

races_with_timestamp=add_ingestion_date(races_df)
races_race_timestmap=races_with_timestamp.withColumn('race_timestamp',to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

display(races_with_timestamp)

# COMMAND ----------

# MAGIC %md
# MAGIC **select only required columns**
# MAGIC
# MAGIC

# COMMAND ----------

races_selected_col=races_race_timestmap.withColumn("data_source", lit(v_data_source)).select(col('raceId').alias('race_id'),col('year').alias('race_year'),col('round'),col('circuitId').alias('circuit_id'),col('name'),col('ingestion_date'),col('race_timestamp'),col("data_source"))

# COMMAND ----------

display(races_selected_col)

# COMMAND ----------

# MAGIC %md
# MAGIC **write this data to processed container**

# COMMAND ----------

races_selected_col.write.mode('overwrite').partitionBy('race_year').parquet(f'{processed_folder_path}/races')

# COMMAND ----------

display(spark.read.parquet(f'{processed_folder_path}/races'))

# COMMAND ----------

dbutils.notebook.exit("success")