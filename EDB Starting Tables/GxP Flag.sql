-- Databricks notebook source
select * from gms_us_lake.ref_gmsgqmi_src_system_info

-- COMMAND ----------

select * from gms_us_lake.ref_gmsgqmi_src_system_info

-- COMMAND ----------

select * from gms_us_hub.ref_gmsgqddm_src_system_info

-- COMMAND ----------

select * from gms_us_hub.ref_gmsgqddm_src_system_info
where table_name like '%TRACK%'

-- COMMAND ----------

select * from gms_us_hub.ref_gmsgqddm_src_system_info
-- where table_name like 'gms_us_hub%'
