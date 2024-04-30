-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_presentation 
LOCATION "/mnt/formula1dldatastorage/presentation"

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_presentation.race_results
(
  race_year INT,
  race_name STRING,
  race_date DATE,
  circuit_location STRING,
  driver_name STRING,
  driver_number INT,
  driver_nationality STRING,
  team STRING,
  grid INT,
  fastest_lap INT,
  race_time STRING,
  points INT,
  position INT,
  created_date DATE
)


-- COMMAND ----------

DROP TABLE IF EXISTS f1_presentation.driver_standings;
CREATE TABLE IF NOT EXISTS f1_presentation.driver_standings
(
  race_year INT,
  driver_name STRING,
  driver_nationality STRING,
  team STRING,
  total_points INT,
  wins INT,
  rank int
)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_presentation.constructor_standings;
CREATE TABLE IF NOT EXISTS f1_presentation.constructor_standings
(
  race_year INT,
  team STRING,
  total_points INT,
  wins INT,
  rank int
)

-- COMMAND ----------

INSERT OVERWRITE TABLE f1_presentation.race_results
SELECT * FROM parquet.`/mnt/formula1dldatastorage/presentation/race_results`

-- COMMAND ----------

INSERT OVERWRITE TABLE f1_presentation.driver_standings
SELECT * FROM parquet.`/mnt/formula1dldatastorage/presentation/driver_standings`

-- COMMAND ----------

INSERT OVERWRITE TABLE f1_presentation.constructor_standings
SELECT * FROM parquet.`/mnt/formula1dldatastorage/presentation/constructor_standings`

-- COMMAND ----------

select * from f1_presentation.driver_standings
where rank=1;


-- COMMAND ----------

select * from f1_presentation.constructor_standings
where rank=1;