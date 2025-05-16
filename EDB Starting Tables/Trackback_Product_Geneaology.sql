-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Immunate S/D
-- MAGIC

-- COMMAND ----------

select distinct
hier_num
-- ,mfg_seq_num
,root_batch_id
,parent_batch_id
-- ,cmpnt_batch_id
,active_flag
-- ,*
from gms_us_hub.ref_track_back_erp_glbl
 where root_batch_id like 'C3B081 %'
--  where root_batch_id like 'C3B081AA'
 and active_flag like 'Y'
 and (parent_batch_id like 'CA%' OR parent_batch_id like 'CB%' OR parent_batch_id like 'CA%' OR parent_batch_id like 'VIPL%' )
 and (hier_num like '2' OR hier_num like '3' OR hier_num like '4')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Humanalbumin A4

-- COMMAND ----------

select distinct
hier_num
,root_batch_id
,parent_batch_id
-- ,cmpnt_batch_id
,active_flag
from gms_us_hub.ref_track_back_erp_glbl
 where root_batch_id like 'A4B255AA%'
 and active_flag like 'Y'
 and (parent_batch_id like 'AN%' OR parent_batch_id like 'AF%' OR parent_batch_id like 'AL%' OR parent_batch_id like 'EL%' OR parent_batch_id like 'VIPL%' )
 and (hier_num like '2' OR hier_num like '3' OR hier_num like '4' OR hier_num like '5' OR hier_num like '6'  OR hier_num like '7')

-- COMMAND ----------

select distinct
hier_num
,root_batch_id
,parent_batch_id
-- ,cmpnt_batch_id
,active_flag
,*
from gms_us_hub.ref_track_back_erp_glbl
 where root_batch_id like 'ANC%'
-- OR LEFT(root_lot_num, 2) IN ('EL', 'AN', 'AL', 'FN', 'FE', 'FA', 'CF', 'CG', 'CE', 'DK', 'BF', 'BB', 'BK', 'BH', 'BQ', 'BP', 'BN', 'CB', 'KE', 'CA', 'DD', 'DA', 'DN', 'PL')
-- )
--  and active_flag like 'Y'
--  and (parent_batch_id like 'AN%' OR parent_batch_id like 'AF%' OR parent_batch_id like 'AL%' OR parent_batch_id like 'EL%' OR parent_batch_id like 'VIPL%' )
--  and (hier_num like '2' OR hier_num like '3' OR hier_num like '4' OR hier_num like '5' OR hier_num like '6'  OR hier_num like '7')

-- COMMAND ----------

select distinct
-- cmpnt_batch_id
-- ,parent_batch_id
-- ,root_batch_id
-- LEFT(cmpnt_batch_id, 2) AS cmpnt_batch_id_prefix
LEFT(root_batch_id, 4) AS root_batch_id_prefix
from gms_us_hub.ref_track_forward_erp_glbl
where root_batch_id like '%PL%'
-- and cmpnt_batch_id like 'EL%'
AND
(LEFT(cmpnt_batch_id, 2) IN ('EL', 'AN', 'AL', 'FN', 'FE', 'FA', 'CF', 'CG', 'CE', 'DK', 'BF', 'BB', 'BK', 'BH', 'BQ', 'BP', 'BN', 'CB', 'KE', 'CA', 'DD', 'DA', 'DN')) --23 different prefixes
