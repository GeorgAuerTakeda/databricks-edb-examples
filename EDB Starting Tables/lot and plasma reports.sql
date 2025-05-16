-- Databricks notebook source
-- MAGIC %md
-- MAGIC # lot report plasma **report**

-- COMMAND ----------

select distinct
lot_num as batch_id,
MFG_DT,
PARENT_MFG_Dt,
STORAGE_MTH_TIME,
PLASMA_RLS_DT,
PROD_QTY,
MFG_FIRST_DT,
MFG_LAST_DT,
INV_HOLD_DAYS,
PLASMA_VOL,
DONATION_COUNT,
DONOR_COUNT,
-- AGING_MAX_DAYS,
-- AGING_MIN_DAYS,
-- AGING_PLASMA_TIME,
BLEED_FIRST_DT,
BLEED_LAST_DT,
*
from gms_us_mart.ref_gmsgq_plasma_lot_genealogy_glbl
-- where root_lot_num like 'A4B%'
where lot_num like 'VIPLB%'
and root_lot_num like 'A4B256%'
-- where root_lot_num like 'A4B255%'

-- COMMAND ----------

select * from gms_us_mart.ref_gmsgq_plasma_lot_genealogy_glbl
-- where root_lot_num like 'A4B%'
where lot_num like 'VIPLB0105B%'

-- where root_lot_num like 'A4B255%'
