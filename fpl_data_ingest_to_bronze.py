# Databricks notebook source
# MAGIC %md
# MAGIC ## Fantasy Football ETL

# COMMAND ----------

# MAGIC %md
# MAGIC ###Download the data from the FPL API

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import java.net.URL
# MAGIC import java.io.File
# MAGIC import org.apache.commons.io.FileUtils
# MAGIC 
# MAGIC val tmpFile = new File("/tmp/fpl.json")
# MAGIC FileUtils.copyURLToFile(new URL("https://fantasy.premierleague.com/api/bootstrap-static/"), tmpFile)

# COMMAND ----------

basePath = "dbfs:/users/rishi.ghose@databricks.com/fpl"

dbutils.fs.cp('file:/tmp/fpl.json', basePath + '/raw/fpl.json')
fpl_path = basePath + '/raw/fpl.json'

dbutils.fs.head(fpl_path)

# COMMAND ----------

fpl_df = spark.read\
  .option("inferschema", True)\
  .option("multiline", True)\
  .json(fpl_path)

fpl_df.printSchema()

# COMMAND ----------

fpl_df.createOrReplaceTempView("fpl_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create the Database and set other configurations

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

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS ${c.databaseName}
# MAGIC   COMMENT "This database stores fantasy premier league data"
# MAGIC   LOCATION "${c.deltaPath}/database";

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Explore the player data and create table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT players.* 
# MAGIC FROM
# MAGIC   (SELECT explode(elements) players FROM fpl_data)
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create the Player table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS ${c.databaseName}.players_bronze;
# MAGIC 
# MAGIC CREATE TABLE ${c.databaseName}.players_bronze
# MAGIC    USING DELTA
# MAGIC    LOCATION "${c.deltaPath}/players_bronze"
# MAGIC    COMMENT "premier league player information"
# MAGIC    AS (
# MAGIC      SELECT players.* 
# MAGIC      FROM (SELECT explode(elements) players FROM fpl_data)
# MAGIC    );

# COMMAND ----------

import pyspark.sql.functions as F

top10_form = (
  fpl_df.select(F.explode("elements").alias("players"))
  .selectExpr("players.form", "players.code", "players.web_name as name")
  .orderBy(F.col("form").desc())
  .limit(10)
)

display(top10_form)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Explore the player data

# COMMAND ----------

import numpy as np

topplayers = np.array(top10_form.collect())

script = ""
for form, code, name in topplayers:
  script += f"""
  <tr>
    <td style="border: 1px solid black"><img src="https://resources.premierleague.com/premierleague/photos/players/110x140/p{code}.png" height="70"/></td>
    <td style="border: 1px solid black; text-align: left">
      <p>{name}</p>
      <p>form : {form}</p>
    </td>
  </tr>
  """
  
displayHTML(
  """
  <p style="color:blue; font-weight:bold">Top 10 players by form</p>
  <table>
  """ +
  script +
  "</table>"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Get Player Metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT et.* 
# MAGIC FROM
# MAGIC   (SELECT explode(element_types) et FROM fpl_data)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS ${c.databaseName}.player_metadata_bronze;
# MAGIC 
# MAGIC CREATE TABLE ${c.databaseName}.player_metadata_bronze
# MAGIC    USING parquet
# MAGIC    LOCATION "${c.deltaPath}/player_metadata_bronze"
# MAGIC    COMMENT "premier league player types"
# MAGIC    AS (
# MAGIC      SELECT et.* 
# MAGIC      FROM (SELECT explode(element_types) et FROM fpl_data)
# MAGIC    );

# COMMAND ----------

# MAGIC %md
# MAGIC ###Explore the Team Data

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT teams.* 
# MAGIC FROM
# MAGIC   (SELECT explode(teams) teams FROM fpl_data)
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create the Team Table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS ${c.databaseName}.teams_bronze;
# MAGIC 
# MAGIC CREATE TABLE ${c.databaseName}.teams_bronze
# MAGIC    USING delta
# MAGIC    LOCATION "${c.deltaPath}/teams_bronze"
# MAGIC    COMMENT "premier league teams"
# MAGIC    AS (
# MAGIC      SELECT teams.* 
# MAGIC      FROM (SELECT explode(teams) teams FROM fpl_data)
# MAGIC    );

# COMMAND ----------

# MAGIC %md
# MAGIC ###Explore Events Data

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT events.* 
# MAGIC FROM
# MAGIC   (SELECT explode(events) events FROM fpl_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create the events table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS ${c.databaseName}.events_bronze;
# MAGIC 
# MAGIC CREATE TABLE ${c.databaseName}.events_bronze
# MAGIC    USING delta
# MAGIC    LOCATION "${c.deltaPath}/events_bronze"
# MAGIC    COMMENT "premier league events"
# MAGIC    AS (
# MAGIC      SELECT events.* 
# MAGIC      FROM (SELECT explode(events) events FROM fpl_data)
# MAGIC    );

# COMMAND ----------


