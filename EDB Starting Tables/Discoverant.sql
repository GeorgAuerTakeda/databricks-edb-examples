-- Databricks notebook source
-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC
-- MAGIC # kn_link = 'http://discoverant-pd.onetakeda.com:18080/discoverant-kn/Results/kariemh/FC_Sterile+Filtration+Flow/Daily+Job/Table+View+%28FC_Sterile+Filtration+Flow%29.html'
-- MAGIC
-- MAGIC tables = pd.read_html(kn_link)
-- MAGIC
-- MAGIC df = tables[4]

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_spark = spark.createDataFrame(df)
-- MAGIC # Rename columns to remove invalid characters
-- MAGIC # df_spark = df_spark.toDF(*[
-- MAGIC #     c.replace(' ', '_')
-- MAGIC #      .replace(';', '_')
-- MAGIC #      .replace('{', '_')
-- MAGIC #      .replace('}', '_')
-- MAGIC #      .replace('(', '_')
-- MAGIC #      .replace(')', '_')
-- MAGIC #      .replace('\n', '_')
-- MAGIC #      .replace('\t', '_')
-- MAGIC #      .replace('=', '_')
-- MAGIC #     for c in df_spark.columns
-- MAGIC # ])
-- MAGIC # df_spark.write.saveAsTable("gms_us_alyt.new_table_name")
-- MAGIC display(df_spark)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(tables[1])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(tables[2])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(tables[3])
