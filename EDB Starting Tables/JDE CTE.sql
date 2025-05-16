-- Databricks notebook source
-- MAGIC %md
-- MAGIC # sales number and order number

-- COMMAND ----------

select 
BATCH_ID
, REL_ORD_NUM
, SO_NUM
, * 
from gms_us_hub.txn_salesorder_erp_glbl
where ACTIVE_FLAG = 'Y'
and REL_ORD_TYPE_CD = 'WO'
and PLANT_ID like '%VN%'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # this notebook uses CTEs, common table expressions - and combines those CTEs in a final table for further use
-- MAGIC

-- COMMAND ----------

-- Common Table Expressions (CTEs) are temporary result sets that are defined within the execution scope of a single SQL statement.
-- They are created using the WITH clause and can be referenced within the main query.
-- Advantages of CTEs:
-- 1. Improved Readability: CTEs make complex queries easier to read and understand by breaking them into simpler, reusable components.
-- 2. Modularity: CTEs allow you to define and reuse subqueries within the main query, promoting modularity.
-- 3. Recursion: CTEs support recursive queries, which are useful for hierarchical or tree-structured data.
-- 4. Simplified Maintenance: CTEs can simplify query maintenance by isolating different parts of the query logic.

-- COMMAND ----------

WITH 
mb_filtered as (
  SELECT
  MATERIALBATCH_KEY,
  BATCH_ID,
  MAT_DESC,
  MAT_ID,
  BATCH_PROD_DT,
  SHELF_LIFE_EXPIRE_DT
  FROM gms_us_hub.ref_materialbatch_erp_glbl --JDE: F4108 "Lot Master"
),
-- pmv_filtered AS (
--   SELECT 
--   *
--   FROM gms_us_hub.txn_ppmaterialview_erp_glbl --JDE: F4801
-- ),
-- inv_filtered AS (
--   SELECT MATERIALBATCH_KEY, PLANT_ID, BATCH_STAT_CD, MAT_GROUP_CD, COLLECT_SET(STORAGE_LOCATION_CD) as Storage_Locations
--   FROM gms_us_hub.txn_inventory_erp_glbl --JDE: ?, for mat group & qty on hand
--   WHERE PLANT_ID LIKE '%VN%'
--   GROUP BY MATERIALBATCH_KEY, PLANT_ID, BATCH_STAT_CD, MAT_GROUP_CD
-- ),
f4108 as (
  SELECT 
  IOLOTN as BATCH_ID,
  IOUB04 / 10000 as CompletedQuantity
  FROM gms_us_lake.gmsgq_jde_proddta_F4108_ADT
)
-- select * FROM inv_filtered
SELECT
mb_filtered.BATCH_ID as PrecipitateGLotNumber,
mb_filtered.MAT_DESC as ProductDescription,
mb_filtered.MAT_ID as ItemNumber,
mb_filtered.BATCH_PROD_DT as DateofManufacture,
mb_filtered.SHELF_LIFE_EXPIRE_DT as DateofExpiration,
f4108.CompletedQuantity
FROM 
mb_filtered
-- left JOIN pmv_filtered ON mb_filtered.MATERIALBATCH_KEY = pmv_filtered.MATERIALBATCH_KEY
-- left JOIN inv_filtered ON mb_filtered.MATERIALBATCH_KEY = inv_filtered.MATERIALBATCH_KEY
left JOIN f4108 on mb_filtered.BATCH_ID = f4108.BATCH_ID
WHERE
  -- mb_filtered.BATCH_PROD_DT LIKE '%202401%'
  -- mb_filtered.BATCH_ID like "ELA0808 %"
  mb_filtered.BATCH_ID like "ELB0238 %"
-- AND
  -- mb.BATCH_STAT_CD
  -- NOT (inv_filtered.BATCH_STAT_CD LIKE ' ' OR inv_filtered.BATCH_STAT_CD LIKE 'E')
-- AND
--   pmv_filtered.WO_STAT_CD <= 95
-- WO_NUM is like "5634861%"

