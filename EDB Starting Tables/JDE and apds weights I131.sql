-- Databricks notebook source
-- MAGIC %md
-- MAGIC # JDE Quantity Lake Table F4108 aka Lot Master

-- COMMAND ----------

with
f4108 as (
  SELECT 
  trim(IOLOTN) as BATCH_ID,
  CAST((IOUB04 / 10000) AS decimal(12, 1)) AS CompletedQuantity,
  *
  FROM gms_us_lake.gmsgq_jde_proddta_F4108_ADT
)
select * from f4108
where batch_id like 'ELB0670'
or batch_id like 'ALB0670'
or batch_id like 'VIPLB0670A'
or batch_id like 'VIPLB0670B'
or batch_id like 'VIPLB0670C'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # lot report neu siehe veeva
-- MAGIC https://takeda-quality.veevavault.com/ui/#doc_info/793517/6/0=&idx=12&pt=rl&sm=&tvsl=Jml2cD0xJml2dj1DT01QQUNUJnZpZXdJZD1yZWNlbnQmdGFiSWQ9MFRCMDAwMDAwMDEzMDA4JmZhY2V0c1VuY2hhbmdlZD1mYWxzZSZpdnM9RG9jTGFzdFZpZXdlZCZpdm89ZGVzYyZpbnRlcm5hbEZpbHRlcnMlNUIlNUQ9X2RvY1ZlcnNpb25GaWx0ZXIlM0ElNUIlMjJ0cnVlJTIyJTVE

-- COMMAND ----------


select lot_num, plasma_vol, * from gms_us_mart.ref_gmsgq_plasma_lot_genealogy_glbl
where lot_num like '%VIPLB0670%'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # alternative zur lake tabelle
-- MAGIC durch die negative zahl beim wegbuchen auch keine besonders saubere lösung, muss erst mit SUM() und ABS() behandelt werden
-- MAGIC txn_ppmaterialview_erp_glbl ist für euch nicht relevant denk ich - ich habs gebraucht weil ich die AL Gewichte in bezug auf die ANs benötigt habe

-- COMMAND ----------

with
qa_vie_a4_quantity_vw AS (
  select
  trim(mv.batch_id) as an_batch_id,
  trim(st.BATCH_ID) as al_batch_id,
  CONCAT_WS('_', trim(mv.batch_id), trim(st.batch_id)) AS an_al_key,
  -- sum of all quantities is calculated
  -- the values only have one digit, and are formatted
  -- absolute value is calculated, since the values are negative
  format_number(ABS(SUM(st.LINE_ITEM_QTY)), 1) as quantity

  -- PRIMARY KEY (mv_batch_id, st_batch_id)
  from gms_us_hub.txn_stockmovement_erp_glbl st
  JOIN gms_us_hub.txn_ppmaterialview_erp_glbl mv
  ON st.WO_NUM = mv.WO_NUM

  where mv.plant_id LIKE '%VN%'
  -- and mv.batch_id LIKE 'AN%'
  and st.batch_id LIKE 'AL%' -- the AL bulk qty are the interesting part
  -- the ALK is no bulk, but alcohol!
  -- AND mv.batch_id LIKE 'ANB112 %'
  group by AL_batch_id, AN_batch_id
)
select * from qa_vie_a4_quantity_vw
where al_batch_id like 'ALB0670'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # apds tabellen für wien
-- MAGIC listet in dieser form einzelspenden auf - achtung wegen 'D' für destroyed

-- COMMAND ----------

WITH lot AS (
  SELECT DISTINCT
  lot_pid,
  lot_code,
  TRIM(lot_number) as lot_number,
  schedule_number
  from gms_us_lake.gmsgqmi_apds_vn_dsp_lot
),
lot_assign AS(
  SELECT
  lot_pid,
  lot_entity_pid
  from
  gms_us_lake.gmsgqmi_apds_vn_pla_unit_lot_assign
),
donordata AS(
  select
  unit_pid,
  bleed_date,
  expected_volume,
  actual_volume,
  donor_number,
  disposition_action
  FROM
    gms_us_lake.gmsgqmi_apds_vn_pla_unit
  WHERE
  disposition_action NOT LIKE 'D' -- D = destroy
  -- where unit_pid like "200000000107590021%" --=lot_entity_pid
)

SELECT
*
-- bleed_date,
-- lot_number,
-- schedule_number,
-- expected_volume,
-- actual_volume,
-- MIN(bleed_date) AS lowest_bleed_date,
-- MAX(bleed_date) AS highest_bleed_date
from
  lot
  LEFT JOIN lot_assign
  ON lot.lot_pid = lot_assign.lot_pid
  LEFT JOIN donordata
  ON lot_assign.lot_entity_pid = donordata.unit_pid
-- unit_pid like "200000000107590021%" --=lot_entity_pid

where
-- lot.schedule_number like "VIPLB0670%"
-- lot.schedule_number like "VIPLB0431%"
lot.schedule_number like "VIPLB0432%"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # sums

-- COMMAND ----------

WITH lot AS (
  SELECT DISTINCT
  lot_pid,
  lot_code,
  TRIM(lot_number) as lot_number,
  schedule_number
  from gms_us_lake.gmsgqmi_apds_vn_dsp_lot
),
lot_assign AS(
  SELECT
  lot_pid,
  lot_entity_pid
  from
  gms_us_lake.gmsgqmi_apds_vn_pla_unit_lot_assign
),
donordata AS(
  select
  unit_pid,
  bleed_date,
  expected_volume,
  actual_volume,
  donor_number,
  disposition_action
  FROM
    gms_us_lake.gmsgqmi_apds_vn_pla_unit
  WHERE
  disposition_action NOT LIKE 'D' -- D = destroy
  -- where unit_pid like "200000000107590021%" --=lot_entity_pid
),
donordata_with_destroy  AS(
  select
  unit_pid,
  bleed_date,
  expected_volume,
  actual_volume,
  donor_number,
  disposition_action
  FROM
    gms_us_lake.gmsgqmi_apds_vn_pla_unit
  -- WHERE
  -- disposition_action NOT LIKE 'D' -- D = destroy
  -- where unit_pid like "200000000107590021%" --=lot_entity_pid
)

SELECT
-- bleed_date,
-- lot_number,
schedule_number,
SUM(CAST(donordata.expected_volume AS decimal(10, 3))) as donordata_expected_volume,
SUM(CAST(donordata.actual_volume AS decimal(10, 3))) as donordata_actual_volume,
SUM(CAST(donordata_with_destroy.expected_volume AS decimal(10, 3))) as donordata_with_destroy_expected_volume,
(SUM(CAST(donordata.expected_volume AS decimal(10, 3))) - SUM(CAST(donordata_with_destroy.expected_volume AS decimal(10, 3)))) as delta_expected_volume,
(SUM(CAST(donordata.actual_volume AS decimal(10, 3))) - SUM(CAST(donordata_with_destroy.actual_volume AS decimal(10, 3)))) as delta_actual_volume,
SUM(CAST(donordata_with_destroy.actual_volume AS decimal(10, 3))) as donordata_with_destroy_actual_volume
-- MIN(bleed_date) AS lowest_bleed_date,
-- MAX(bleed_date) AS highest_bleed_date
from
  lot
  LEFT JOIN lot_assign
  ON lot.lot_pid = lot_assign.lot_pid
  LEFT JOIN donordata_with_destroy
  ON lot_assign.lot_entity_pid = donordata_with_destroy.unit_pid
  LEFT JOIN donordata
  ON lot_assign.lot_entity_pid = donordata.unit_pid
where
lot.schedule_number like "VIPL%"
-- lot.schedule_number like "VIPLB0670%"
group BY schedule_number
-- unit_pid like "200000000107590021%" --=lot_entity_pid

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # number of 'D' donations per lot

-- COMMAND ----------

WITH lot AS (
  SELECT DISTINCT
    lot_pid,
    lot_code,
    TRIM(lot_number) as lot_number,
    schedule_number
  FROM gms_us_lake.gmsgqmi_apds_vn_dsp_lot
),
lot_assign AS (
  SELECT
    lot_pid,
    lot_entity_pid
  FROM gms_us_lake.gmsgqmi_apds_vn_pla_unit_lot_assign
),
donordata AS (
  SELECT
    unit_pid,
    bleed_date,
    expected_volume,
    actual_volume,
    donor_number,
    disposition_action
  FROM gms_us_lake.gmsgqmi_apds_vn_pla_unit
  WHERE disposition_action NOT LIKE 'D' -- D = destroy
),
donordata_with_destroy AS (
  SELECT
    unit_pid,
    bleed_date,
    expected_volume,
    actual_volume,
    donor_number,
    disposition_action
  FROM gms_us_lake.gmsgqmi_apds_vn_pla_unit
),
d_count AS (
  SELECT
    unit_pid,
    COUNT(*) AS d_rows_count
  FROM gms_us_lake.gmsgqmi_apds_vn_pla_unit
  WHERE disposition_action = 'D'
  GROUP BY unit_pid
)

SELECT
  schedule_number,
  SUM(CAST(donordata.expected_volume AS decimal(10, 3))) as donordata_expected_volume,
  SUM(CAST(donordata.actual_volume AS decimal(10, 3))) as donordata_actual_volume,
  SUM(CAST(donordata_with_destroy.expected_volume AS decimal(10, 3))) as donordata_with_destroy_expected_volume,
  (SUM(CAST(donordata.expected_volume AS decimal(10, 3))) - SUM(CAST(donordata_with_destroy.expected_volume AS decimal(10, 3)))) as delta_expected_volume,
  (SUM(CAST(donordata.actual_volume AS decimal(10, 3))) - SUM(CAST(donordata_with_destroy.actual_volume AS decimal(10, 3)))) as delta_actual_volume,
  SUM(CAST(donordata_with_destroy.actual_volume AS decimal(10, 3))) as donordata_with_destroy_actual_volume,
  SUM(d_count.d_rows_count) as d_rows_count
FROM lot
  LEFT JOIN lot_assign ON lot.lot_pid = lot_assign.lot_pid
  LEFT JOIN donordata_with_destroy ON lot_assign.lot_entity_pid = donordata_with_destroy.unit_pid
  LEFT JOIN donordata ON lot_assign.lot_entity_pid = donordata.unit_pid
  LEFT JOIN d_count ON lot_assign.lot_entity_pid = d_count.unit_pid
WHERE lot.schedule_number LIKE "VIPL%"
GROUP BY schedule_number
