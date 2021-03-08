# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest FPL fixture data

# COMMAND ----------

import json
import urllib.request

url = "https://fantasy.premierleague.com/api/fixtures/"
data = urllib.request.urlopen(url).read().decode()

fixtures = json.loads(data)

# COMMAND ----------

with open('/tmp/fixtures.json', 'w') as f:
  json.dump(fixtures, f)

basePath = "dbfs:/users/rishi.ghose@databricks.com/fpl"

dbutils.fs.cp('file:/tmp/fixtures.json', basePath + '/raw/fixtures.json')
fpl_path = basePath + '/raw/fixtures.json'

dbutils.fs.head(fpl_path)

# COMMAND ----------

fixtures_df = spark.read\
  .option('multiline', True)\
  .option('inferSchema', True)\
  .json(fpl_path)

fixtures_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Initialise variables and paths

# COMMAND ----------

# Dropdown to get databse name
dbutils.widgets.text("database", "premier_league", "Database Name")

user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user') #current user
database = dbutils.widgets.get("database") #database name

#path to store the database and delta tables
deltapath = f"dbfs:/Users/{user}/{database}"
#adding the database name as configuration to access property through SQL 
spark.conf.set("c.databaseName", database)
spark.conf.set("c.deltaPath", deltapath)

print(f"{deltapath}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Fixtures Bronze Table

# COMMAND ----------

fixtures_df.write.format("delta").save(deltapath+"/fixture_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create Fixtures Silver Table

# COMMAND ----------

fixtures_df.createOrReplaceTempView("fixtures")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE ${c.databaseName}; 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS ${c.databaseName}.fixtures_silver;
# MAGIC 
# MAGIC CREATE TABLE ${c.databaseName}.fixtures_silver
# MAGIC USING DELTA
# MAGIC LOCATION "${c.deltaPath}/fixtures_silver}"
# MAGIC COMMENT "Premier League Fixtures"
# MAGIC AS (
# MAGIC SELECT 
# MAGIC   event,
# MAGIC   finished,
# MAGIC   id,
# MAGIC   CAST(to_timestamp(kickoff_time, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS timestamp) event_time,
# MAGIC   team_a,
# MAGIC   team_h,
# MAGIC   team_a_score,
# MAGIC   team_h_score,
# MAGIC   IF(team_a_difficulty > team_h_difficulty, team_a, team_h) AS stronger_team
# MAGIC FROM fixtures
# MAGIC WHERE 
# MAGIC   event IS NOT NULL
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM ${c.databaseName}.fixtures_silver

# COMMAND ----------


