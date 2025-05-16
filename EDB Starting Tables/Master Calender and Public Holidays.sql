-- Databricks notebook source
select * from gms_us_hub.ref_cldr_glbl

-- COMMAND ----------

-- MAGIC %python
-- MAGIC !pip install azureml-opendatasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.library.restartPython()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from azureml.opendatasets import PublicHolidays
-- MAGIC
-- MAGIC from datetime import datetime
-- MAGIC from dateutil import parser
-- MAGIC from dateutil.relativedelta import relativedelta
-- MAGIC
-- MAGIC
-- MAGIC end_date = datetime.today()
-- MAGIC start_date = datetime.today() - relativedelta(months=1)
-- MAGIC hol = PublicHolidays(start_date=start_date, end_date=end_date)
-- MAGIC hol_df = hol.to_pandas_dataframe()
-- MAGIC
-- MAGIC # Convert pandas dataframe to pyspark dataframe
-- MAGIC hol_df = spark.createDataFrame(hol_df)
-- MAGIC display(hol_df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # create table in alyt

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Ensure hol_df is a PySpark DataFrame
-- MAGIC # hol_df = spark.createDataFrame(hol_df)
-- MAGIC
-- MAGIC # Register the DataFrame as a temporary view
-- MAGIC hol_df.createOrReplaceTempView("hol_df_view")
-- MAGIC
-- MAGIC # Now you can use SQL to create the table
-- MAGIC spark.sql("""
-- MAGIC create table gms_us_alyt.austrian_holidays as
-- MAGIC select * from hol_df_view
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # create temp table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Ensure hol_df is a PySpark DataFrame
-- MAGIC # hol_df = spark.createDataFrame(hol_df)
-- MAGIC
-- MAGIC # Register the DataFrame as a temporary view
-- MAGIC hol_df.createOrReplaceTempView("hol_df_view")
-- MAGIC
-- MAGIC # Now you can use SQL to create the temporary table
-- MAGIC spark.sql("""
-- MAGIC create or replace temporary view austrian_holidays as
-- MAGIC select * from hol_df_view
-- MAGIC """)

-- COMMAND ----------

select * from austrian_holidays
-- where countryRegionCode like 'AT'
-- where countryOrRegion like 'Austria'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Azure approach
-- MAGIC https://learn.microsoft.com/en-us/azure/open-datasets/dataset-public-holidays?tabs=pyspark

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Azure storage access info
-- MAGIC blob_account_name = "azureopendatastorage"
-- MAGIC blob_container_name = "holidaydatacontainer"
-- MAGIC blob_relative_path = "Processed"
-- MAGIC blob_sas_token = r""

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Allow SPARK to read from Blob remotely
-- MAGIC wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path)
-- MAGIC spark.conf.set(
-- MAGIC   'fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name),
-- MAGIC   blob_sas_token)
-- MAGIC print('Remote blob path: ' + wasbs_path)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # SPARK read parquet, note that it won't load any data yet by now
-- MAGIC df = spark.read.parquet(wasbs_path)
-- MAGIC print('Register the DataFrame as a SQL temporary view: source')
-- MAGIC df.createOrReplaceTempView('source')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import expr, lit
-- MAGIC
-- MAGIC # Add missing holidays for Takeda Austria according to KV
-- MAGIC additional_holidays = [
-- MAGIC     ('AT', 'Heiligabend', 'Heiligabend', 'Austria', f'{year}-12-24 00:00:00') for year in range(1970, 2099)
-- MAGIC ] + [
-- MAGIC     ('AT', 'Silvester', 'Silvester', 'Austria', f'{year}-12-31 00:00:00') for year in range(1970, 2099)
-- MAGIC ]
-- MAGIC
-- MAGIC # Adjust the schema to match the number of columns in additional_holidays
-- MAGIC schema = ['countryRegionCode', 'holidayName', 'normalizeHolidayName', 'countryorRegion', 'date']
-- MAGIC
-- MAGIC additional_holidays_df = spark.createDataFrame(additional_holidays, schema=schema)
-- MAGIC
-- MAGIC # Select only the relevant columns from the original DataFrame
-- MAGIC df = df.select('countryRegionCode', 'holidayName', 'normalizeHolidayName', 'countryorRegion', 'date')
-- MAGIC
-- MAGIC # Find the date of "Fronleichnam" and add one day for "Betriebsurlaub"
-- MAGIC fronleichnam_df = df.filter((df.holidayName == 'Fronleichnam') & (df.countryRegionCode == 'AT'))
-- MAGIC betriebsurlaub_df = fronleichnam_df.withColumn('date', fronleichnam_df['date'] + expr('INTERVAL 1 DAY')) \
-- MAGIC                                    .withColumn('holidayName', lit('Betriebsurlaub')) \
-- MAGIC                                    .withColumn('normalizeHolidayName', lit('Betriebsurlaub'))
-- MAGIC
-- MAGIC # Union the additional holidays and betriebsurlaub_df with the original DataFrame
-- MAGIC df = df.union(additional_holidays_df).union(betriebsurlaub_df)
-- MAGIC df.createOrReplaceTempView('source')
-- MAGIC
-- MAGIC # Display Austrian holidays
-- MAGIC display(spark.sql("SELECT * FROM source WHERE countryRegionCode = 'AT'"))
-- MAGIC
-- MAGIC # Create or replace table with the updated DataFrame
-- MAGIC spark.sql("""
-- MAGIC create or replace table gms_us_alyt.qa_vie_austrian_holidays as
-- MAGIC select * from source
-- MAGIC """)

-- COMMAND ----------

select * from gms_us_alyt.qa_vie_austrian_holidays
where countryRegionCode like 'AT'
sort by date asc
