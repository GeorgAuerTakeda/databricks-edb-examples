-- Databricks notebook source
-- MAGIC %md
-- MAGIC # batch release date or batch cpa date table

-- COMMAND ----------

with
stock_mvmt as
(
select batch_id, 
        CASE 
            WHEN (mvmt_type_cd = 'IZ' AND TRIM(batch_stat_cd) = '') THEN MIN(TO_DATE(wo_trans_dt, 'yyyyMMdd')) 
        END AS disp_dt, -- If the Batch is first set to released '' with an IZ Booking, the transaction date from this event is the release date
        CASE 
            WHEN (mvmt_type_cd = 'IZ' AND TRIM(batch_stat_cd) = 'A') THEN MIN(TO_DATE(wo_trans_dt, 'yyyyMMdd')) 
        END AS a_disp_dt -- If the Batch is first set to released '' with an IZ Booking, the transaction date from this event is the CPA date (A-Setzung, Status A)
       from gms_us_hub.txn_stockmovement_erp_glbl
       where WO_TRANS_DT > 20250101
       group by batch_id, mvmt_type_cd, batch_stat_cd
)
select
       batch_id,
      --  wo_trans_dt,
       MAX(disp_dt) as disp_dt,
       MAX(a_disp_dt) as a_disp_dt
from stock_mvmt group by batch_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # checking batch released in last few days

-- COMMAND ----------

select batch_id, mvmt_type_cd, wo_trans_dt, * from gms_us_hub.txn_stockmovement_erp_glbl
where batch_id like 'ALB0103%'

-- COMMAND ----------

with
stock_mvmt as
(
select batch_id, 
       -- mvmt_type_cd, 
       -- min(wo_trans_dt) as wo_trans_dt,
       -- batch_stat_cd,
        CASE 
            WHEN (mvmt_type_cd = 'IZ' AND TRIM(batch_stat_cd) = '') THEN MIN(TO_DATE(wo_trans_dt, 'yyyyMMdd')) 
        END AS disposition_dt -- If the Batch is first set to released '' with an IZ Booking, the transaction date from this event is the release date
       from gms_us_hub.txn_stockmovement_erp_glbl
      --  where batch_id like 'ANB2%' 
       where plant_id like '%VN%'
       group by batch_id, mvmt_type_cd, batch_stat_cd
)
select
       batch_id,
       MIN(disposition_dt) as disposition_dt
from stock_mvmt
WHERE (disposition_dt > (current_date() - INTERVAL 7 DAYS)) -- filter for batches that where dispositioned in the last seven days
group by batch_id

-- COMMAND ----------

with
stock_mvmt as
(
select batch_id, 
       -- mvmt_type_cd, 
       -- min(wo_trans_dt) as wo_trans_dt,
       -- batch_stat_cd,
        CASE 
            WHEN (mvmt_type_cd = 'IZ' AND TRIM(batch_stat_cd) = '') THEN MIN(TO_DATE(wo_trans_dt, 'yyyyMMdd')) 
        END AS disp_dt, -- If the Batch is first set to released '' with an IZ Booking, the transaction date from this event is the release date
        CASE 
            WHEN (mvmt_type_cd = 'IZ' AND TRIM(batch_stat_cd) = 'A') THEN MIN(TO_DATE(wo_trans_dt, 'yyyyMMdd')) 
        END AS a_disp_dt -- If the Batch is first set to released '' with an IZ Booking, the transaction date from this event is the CPA date (A-Setzung, Status A)
       from gms_us_hub.txn_stockmovement_erp_glbl
       -- where batch_id like 'ALB010%' and mvmt_type_cd = 'IZ'
       -- where batch_id like 'ANB200%' 
    --    where batch_id like 'ANB2%' 
       -- where batch_id like 'A4%' 
       -- where batch_id like 'L0000%' and mvmt_type_cd = 'IZ'
       group by batch_id, mvmt_type_cd, batch_stat_cd
)
select
       batch_id,
       MAX(disp_dt) as disp_dt,
       MAX(a_disp_dt) as a_disp_dt
from stock_mvmt group by batch_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # filter out batches where the disp date is more than 7 days ago

-- COMMAND ----------

with
stock_mvmt as
(
select
       batch_id, 
       -- batch_stat_cd,
       --  CASE 
       --      WHEN (TRIM(batch_stat_cd) = '') THEN MIN(TO_DATE(wo_trans_dt, 'yyyyMMdd')) 
       --  END AS batch_stat_cd,
        CASE 
            WHEN (mvmt_type_cd = 'IZ' AND TRIM(batch_stat_cd) = '') THEN MIN(TO_DATE(wo_trans_dt, 'yyyyMMdd')) 
        END AS disp_dt, -- If the Batch is first set to released '' with an IZ Booking, the transaction date from this event is the release date
        CASE 
            WHEN (mvmt_type_cd = 'IZ' AND TRIM(batch_stat_cd) = 'A') THEN MIN(TO_DATE(wo_trans_dt, 'yyyyMMdd')) 
        END AS a_disp_dt, -- If the Batch is first set to released '' with an IZ Booking, the transaction date from this event is the CPA date
        CASE 
            WHEN (mvmt_type_cd = 'IC') THEN MIN(TO_DATE(wo_trans_dt, 'yyyyMMdd')) 
        END AS creation_dt 
       from gms_us_hub.txn_stockmovement_erp_glbl
       -- where batch_id like 'A4B2%' 
       where batch_id like 'A4B%' 
       group by batch_id, mvmt_type_cd, batch_stat_cd
),
batch_disp_date as (
select
       batch_id,
       -- MAX(batch_stat_cd),
       MAX(creation_dt) as creation_dt,
       MAX(disp_dt) as disp_dt,
       MAX(a_disp_dt) as a_disp_dt
from stock_mvmt 
group by batch_id
-- HAVING MAX(disp_dt) > current_date() - INTERVAL 7 DAYS
)
select * from batch_disp_date
WHERE (disp_dt > (current_date() - INTERVAL 7 DAYS) OR disp_dt is null) -- filter for batches that where disp_dt less than 7 days ago or are not yet dispositioned (null)
and creation_dt > (current_date() - INTERVAL 1 YEAR)
sort by disp_dt asc

-- COMMAND ----------

describe gms_us_hub.txn_stockmovement_erp_glbl
