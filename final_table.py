# Databricks notebook source
# MAGIC %sql
# MAGIC WITH cte AS (
# MAGIC   SELECT 
# MAGIC     ft.pos_site_id,
# MAGIC     hpo.site_label,
# MAGIC     ft.sku_id,
# MAGIC     hpr.sku_label,
# MAGIC     ft.price_substate_id,
# MAGIC     hpri.substate_label,
# MAGIC     ft.fscldt_id,
# MAGIC     hcl.fsclwk_id,
# MAGIC     hw.fsclwk_label,
# MAGIC     ft.type,
# MAGIC     ft.sales_units, 
# MAGIC     ft.sales_dollars,  
# MAGIC     ft.discount_dollars
# MAGIC   FROM 
# MAGIC     gsadb2.default.fact_transactions ft 
# MAGIC     JOIN gsadb2.default.hier_possite hpo ON ft.pos_site_id = hpo.site_id
# MAGIC     JOIN gsadb2.default.hier_prod hpr ON ft.sku_id = hpr.sku_id
# MAGIC     JOIN gsadb2.default.hier_pricestate hpri ON ft.price_substate_id = hpri.substate_id
# MAGIC     JOIN gsadb2.default.hier_clnd hcl ON ft.fscldt_id = hcl.fscldt_id
# MAGIC     JOIN gsadb2.default.hier_wk hw ON hcl.fsclwk_id = hw.fsclwk_id
# MAGIC )
# MAGIC SELECT 
# MAGIC   site_label,
# MAGIC   sku_label,
# MAGIC   substate_label,
# MAGIC   fsclwk_label,
# MAGIC   type,
# MAGIC   SUM(sales_units) AS total_sales_units,
# MAGIC   SUM(sales_dollars) AS total_sales_dollars,
# MAGIC   SUM(discount_dollars) AS total_discount_dollars
# MAGIC FROM 
# MAGIC   cte
# MAGIC GROUP BY 
# MAGIC   site_label,
# MAGIC   sku_label,
# MAGIC   substate_label,
# MAGIC   fsclwk_label,
# MAGIC   type

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH LIVE TABLE mview_weekly_sales AS
# MAGIC WITH cte AS (
# MAGIC   SELECT 
# MAGIC     ft.pos_site_id,
# MAGIC     hpo.site_label,
# MAGIC     ft.sku_id,
# MAGIC     hpr.sku_label,
# MAGIC     ft.price_substate_id,
# MAGIC     hpri.substate_label,
# MAGIC     ft.fscldt_id,
# MAGIC     hcl.fsclwk_id,
# MAGIC     hw.fsclwk_label,
# MAGIC     ft.type,
# MAGIC     ft.sales_units, 
# MAGIC     ft.sales_dollars,  
# MAGIC     ft.discount_dollars
# MAGIC   FROM 
# MAGIC     gsadb2.default.fact_transactions ft 
# MAGIC     JOIN gsadb2.default.hier_possite hpo ON ft.pos_site_id = hpo.site_id
# MAGIC     JOIN gsadb2.default.hier_prod hpr ON ft.sku_id = hpr.sku_id
# MAGIC     JOIN gsadb2.default.hier_pricestate hpri ON ft.price_substate_id = hpri.substate_id
# MAGIC     JOIN gsadb2.default.hier_clnd hcl ON ft.fscldt_id = hcl.fscldt_id
# MAGIC     JOIN gsadb2.default.hier_wk hw ON hcl.fsclwk_id = hw.fsclwk_id
# MAGIC )
# MAGIC SELECT 
# MAGIC   site_label,
# MAGIC   sku_label,
# MAGIC   substate_label,
# MAGIC   fsclwk_label,
# MAGIC   type,
# MAGIC   SUM(sales_units) AS total_sales_units,
# MAGIC   SUM(sales_dollars) AS total_sales_dollars,
# MAGIC   SUM(discount_dollars) AS total_discount_dollars
# MAGIC FROM 
# MAGIC   cte
# MAGIC GROUP BY 
# MAGIC   site_label,
# MAGIC   sku_label,
# MAGIC   substate_label,
# MAGIC   fsclwk_label,
# MAGIC   type