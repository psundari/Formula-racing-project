-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
SELECT team,
       COUNT(1) AS total_races,
       SUM(points) AS total_points,
       AVG(points) AS avg_points,
       RANK() OVER(ORDER BY AVG(points) DESC) team_rank
  FROM f1_presentation.race_results
GROUP BY team
HAVING COUNT(1) >= 100
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT race_year, 
       team,
       COUNT(1) AS total_races,
       SUM(points) AS total_points,
       AVG(points) AS avg_points
  FROM f1_presentation.race_results
 WHERE team IN (SELECT team FROM v_dominant_teams WHERE team_rank <= 5)
GROUP BY race_year, team
ORDER BY race_year, avg_points DESC