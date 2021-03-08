# Databricks notebook source
# MAGIC %md
# MAGIC #Dowload detailed player performance data

# COMMAND ----------

# MAGIC %md
# MAGIC ###Set variables and configurations

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
# MAGIC ###Get player information and download data

# COMMAND ----------

import json
import urllib.request
  
url = f"https://fantasy.premierleague.com/api/element-summary/249/"
data = urllib.request.urlopen(url).read().decode()
info = json.loads(data) 
    
with open('/tmp/player_info.json', 'w') as f:
  json.dump(info, f)

basePath = "dbfs:/users/rishi.ghose@databricks.com/fpl"

dbutils.fs.cp('file:/tmp/player_info.json', basePath + '/raw/player_info.json')
fpl_path = basePath + '/raw/player_info.json'

dbutils.fs.head(fpl_path)

# COMMAND ----------

#read in the schema of the player info file
info_df = spark.read\
  .option('multiline', True)\
  .option('inferSchema', True)\
  .json(fpl_path)

info_df.printSchema()

# COMMAND ----------

@udf(info_df.schema)
def get_player_info(player_id):
  import json
  import urllib.request

  try:
    url = f"https://fantasy.premierleague.com/api/element-summary/{player_id}/"
    data = urllib.request.urlopen(url).read().decode()
    info = json.loads(data) 
  except:
    print("something went wrong")
    
  return info

# COMMAND ----------

players = spark.read.load(deltapath+"/players_bronze")

players.cache().count()

# COMMAND ----------

from pyspark.sql.functions import col

player_info = (
  players.select("id").distinct()
  .withColumn("info", get_player_info("id"))
)

display(player_info)

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

player_info.cache().count()
player_info.createOrReplaceTempView("player_info")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE ${c.databaseName};
# MAGIC 
# MAGIC DROP TABLE IF EXISTS ${c.databaseName}.player_history_silver;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS ${c.databaseName}.player_history_silver
# MAGIC USING DELTA
# MAGIC LOCATION "${c.deltaPath}/player_history_silver"
# MAGIC AS (
# MAGIC   SELECT id player_id, history.* FROM (
# MAGIC     SELECT
# MAGIC       id,
# MAGIC       explode(info.history) history
# MAGIC     FROM player_info
# MAGIC   )
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM ${c.databaseName}.player_history_silver

# COMMAND ----------


