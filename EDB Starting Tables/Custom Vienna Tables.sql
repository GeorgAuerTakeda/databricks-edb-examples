-- Databricks notebook source
-- MAGIC %md
-- MAGIC Extraction done from LIP via JDBC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Vienna specifict tables:
-- MAGIC LIP:
-- MAGIC
-- MAGIC gms_us_alyt.qa_vie_lip_tfivelots, gms_us_alyt.qa_vie_lip_samplecharacterizations, gms_us_alyt.qa_vie_lip_samplingrelevantlotscber, gms_us_alyt.qa_vie_lip_samples, gms_us_alyt.qa_vie_lip_lotcharaterizations, gms_us_alyt.qa_vie_lip_deviations, gms_us_alyt.qa_vie_lip_deliverables, gms_us_alyt.qa_vie_lip_statechanges, gms_us_alyt.qa_vie_lip_deliverablelots, gms_us_alyt.qa_vie_lip_dslist, gms_us_alyt.qa_vie_lip_lotinformation
-- MAGIC
-- MAGIC

-- COMMAND ----------

select lotnumber, itemnumber, affpdays, evaldays, * from gms_us_alyt.qa_vie_lip_lotinformation
where ItemNumber like '7573790'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Batches and the corresponding Basg Certificate Numbers, extracted from PDF via Excel - see Filename

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Data Uploaded via Power Automate Databricks Connector, Extraction from PDFs via Excel

-- COMMAND ----------

select * from gms_us_alyt.qa_vie_basg

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Tables that support QA Disposition specific use cases, for example:

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Table View genereated with JDE, Labware, APDS data

-- COMMAND ----------

select * from gms_us_alyt.qa_vie_precg_vw
