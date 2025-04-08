# Databricks notebook source
df_dim = spark.table(dbutils.widgets.get("src_table_name"))

# Normalize the table by splitting it into two tables: one for unique values and one for the rest
unique_columns = dbutils.widgets.get("unique_columns").split(",")  # Replace with actual unique columns
new_table_primary_key = dbutils.widgets.get("new_table_primary_key")
rest_columns = [col for col in df_dim.columns if col not in unique_columns]
rest_columns.append(new_table_primary_key)

# Create a table for unique values
df_unique = df_dim.select(unique_columns).distinct()
display(df_unique)
df_unique.write.format("delta").mode("overwrite").saveAsTable(dbutils.widgets.get("secondary_dim_table"))
spark.sql(f"ALTER TABLE gsadb2.default.{dbutils.widgets.get('secondary_dim_table')} ALTER COLUMN {dbutils.widgets.get('new_table_primary_key')} SET NOT NULL")
spark.sql(f"ALTER TABLE gsadb2.default.{dbutils.widgets.get('secondary_dim_table')} ADD CONSTRAINT {dbutils.widgets.get('new_table_primary_key')} PRIMARY KEY ({dbutils.widgets.get('new_table_primary_key')})")

# Drop all columns except rest_columns from df_dim
df_rest = df_dim.select(rest_columns)
display(df_rest)
df_rest.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(dbutils.widgets.get("src_table_name"))
spark.sql(f"ALTER TABLE gsadb2.default.{dbutils.widgets.get('src_table_name')} ALTER COLUMN {dbutils.widgets.get('src_table_foriegn_ke')} SET NOT NULL")
spark.sql(f"ALTER TABLE gsadb2.default.{dbutils.widgets.get('src_table_name')} ADD CONSTRAINT {dbutils.widgets.get('src_table_foriegn_ke')} PRIMARY KEY ({dbutils.widgets.get('src_table_foriegn_ke')})")

spark.sql(f"ALTER TABLE gsadb2.default.{dbutils.widgets.get('src_table_name')} ADD CONSTRAINT fk_{dbutils.widgets.get('src_table_foriegn_ke')} FOREIGN KEY ({dbutils.widgets.get('new_table_primary_key')}) REFERENCES gsadb2.default.{dbutils.widgets.get('secondary_dim_table')}({dbutils.widgets.get('new_table_primary_key')})")
