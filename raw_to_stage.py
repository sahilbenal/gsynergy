# Databricks notebook source
dbutils.widgets.text("storage_account_name", "")
dbutils.widgets.text("container_name", "")
dbutils.widgets.text("path", "")
dbutils.widgets.text("header", "")
dbutils.widgets.text("sep", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("primary_key", "")
dbutils.widgets.text("account_key", "")

# COMMAND ----------

storage_account_name = dbutils.widgets.get("storage_account_name")
account_key = dbutils.widgets.get("account_key")
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", account_key)

# COMMAND ----------

storage_account_name = dbutils.widgets.get("storage_account_name")
container_name = dbutils.widgets.get("container_name")
path = dbutils.widgets.get("path")
header = dbutils.widgets.get("header").lower() == 'true'
sep = dbutils.widgets.get("sep")
table_name = dbutils.widgets.get("table_name")
table_name_bad = table_name + "_bad"
primary_key = dbutils.widgets.get("primary_key")

df = spark.read.csv(
    f"abfs://{container_name}@{storage_account_name}.dfs.core.windows.net/{path}",
    header=header,
    sep=sep
)

# Check for nulls or duplicates in the primary key
if primary_key.strip() != '':
    df_corrupt = df.filter(
        df[primary_key].isNull() | 
        df[primary_key].isin(
            [row[primary_key] for row in df.groupBy(primary_key).count().filter("count > 1").collect()]
        )
    )
    df_clean = df.filter(
        ~df[primary_key].isNull() & 
        ~df[primary_key].isin(
            [row[primary_key] for row in df.groupBy(primary_key).count().filter("count > 1").collect()]
        )
    )
else:
    df_clean = df
    df_corrupt = spark.createDataFrame([], df.schema)  # Create an empty DataFrame with the same schema

# Create tables
df_clean.createOrReplaceTempView(f"temp_{table_name}")
spark.sql(f"CREATE or replace TABLE gsadb2.default.{table_name} AS SELECT * FROM temp_{table_name}")

if df_corrupt.count() > 0:
    df_corrupt.createOrReplaceTempView(f"temp_corrupt_{table_name}")
    spark.sql(f"CREATE or replace TABLE gsadb2.default.{table_name_bad} AS SELECT * FROM temp_corrupt_{table_name}")

if primary_key.strip() != '':
    spark.sql(f"ALTER TABLE gsadb2.default.{table_name} ALTER COLUMN {primary_key} SET NOT NULL")
    spark.sql(f"ALTER TABLE gsadb2.default.{table_name} ADD CONSTRAINT {primary_key} PRIMARY KEY ({primary_key})")