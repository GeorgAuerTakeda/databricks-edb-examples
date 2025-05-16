-- Databricks notebook source
-- MAGIC %md
-- MAGIC # combined Crude V Data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC until 17.DEC 2024 CoAs for Crude V where created up to Batch ALB0599

-- COMMAND ----------

-- Labware Data
with GLIMS as(
select
coalesce(mitigation_lotname.T_PH_LOT_NAME, lot_result.LOT_NUMBER) as batch_id -- merge LOT_ERP and LOTNUMBER_VALUE from both systems
, CAST(NUMBER_VALUE AS DECIMAL(10, 1)) AS albumin
from gms_us_mart.txn_lot_result_approved_lims_glbl lot_result
left join gms_us_lake.gmsgq_glims_lot mitigation_lotname
ON lot_result.LOT = mitigation_lotname.LOT_NAME
where analysis like 'VN_PROTEIN_COMPOSI_ALBUMIN'
and testnamedetail like 'Albumin'
and lot_result.ACTIVE_FLAG like 'Y'
),
-- JDE
mb_filtered as (
  SELECT
  MATERIALBATCH_KEY
  , trim(BATCH_ID) as batch_id
  , MAT_DESC as product_description
  , MAT_ID as item_number
  , BATCH_PROD_DT as date_of_manufacture
  , SHELF_LIFE_EXPIRE_DT as date_of_expiration
  FROM gms_us_hub.ref_materialbatch_erp_glbl --JDE: F4108 'Lot Master'
),
f4108 as (
  SELECT 
  trim(IOLOTN) as batch_id
  , CAST((IOUB04 / 10000) AS decimal(12, 4)) AS completed_quantity
  FROM gms_us_lake.gmsgq_jde_proddta_F4108_ADT
),
al_jde_data as (
  SELECT
  mb_filtered.batch_id as batch_id
  , mb_filtered.product_description as product_description
  , mb_filtered.item_number as item_number
  , date_format(to_date(mb_filtered.date_of_manufacture, 'yyyyMMdd'), 'dd-MMM-yyyy') AS date_of_manufacture
  , date_format(to_date(mb_filtered.date_of_expiration, 'yyyyMMdd'), 'dd-MMM-yyyy') AS date_of_expiration
  , f4108.completed_quantity as completed_quantity
  FROM 
  mb_filtered
  left JOIN f4108 on trim(mb_filtered.batch_id) = trim(f4108.batch_id)
  WHERE
    mb_filtered.batch_id like 'AL%'
  AND
    mb_filtered.date_of_manufacture > 20240101
  AND
    f4108.completed_quantity > 0
),
-- JDE plasma pool thawing date
plasma_pool_thawing_date as (
  select
  trim(batch_id) as plasma_batch_id
  , to_date(created_on_dt, 'yyyyMMdd') as thawing_date -- Transaction Date = Thawing Date, is the same for all three plasma sublots
  from gms_us_hub.txn_stockmovement_erp_glbl
  where BATCH_ID like 'VIPL%'
  and mvmt_type_cd like 'IM'
),
-- APDS
F4108 AS(
  SELECT
    trim(IOLOTN) as IOLOTN
    , IOLDSC AS origin_description
    , CAST(LEFT(IOUB04, LEN(IOUB04) - 4) AS DECIMAL(10, 0)) AS plasma_liters
  FROM gms_us_lake.gmsgq_jde_proddta_F4108_ADT
),
trackback AS (
  SELECT distinct
    trim(root_batch_id) AS batch_id
    , trim(parent_batch_id) AS parent_batch_id
    , trim(cmpnt_batch_id) AS cmpnt_batch_id
  FROM gms_us_hub.ref_track_back_erp_glbl
  WHERE
    cmpnt_batch_id LIKE 'VIPL%'
    AND parent_batch_id LIKE 'VIPL%'
    AND root_batch_id LIKE 'AL%' -- ALK is alcohol and not albumin
    AND active_flag like 'Y'
),
apds_lot AS (
  SELECT DISTINCT
    lot_pid
    , lot_code
    , TRIM(lot_number) AS lot_number
    , schedule_number
  FROM gms_us_lake.gmsgqmi_apds_vn_dsp_lot
),
apds_lot_assign AS(
  SELECT distinct
    lot_pid
    , lot_entity_pid
  FROM gms_us_lake.gmsgqmi_apds_vn_pla_unit_lot_assign
),
donordata AS(
  SELECT distinct
    unit_pid
    , bleed_date
    , CAST(actual_volume AS DECIMAL(10, 3)) AS actual_volume
    , donor_number
    , disposition_action
  FROM
    gms_us_lake.gmsgqmi_apds_vn_pla_unit
  WHERE
    disposition_action NOT LIKE 'D'
),
sublotdata AS(
  SELECT distinct
    batch_id
    , parent_batch_id
    , plasma_liters as plasma_volume_after_thawing
    , SUM(actual_volume) as invoiced_plasma_volume
    , date_format(MIN(thawing_date), 'dd-MMM-yyyy') as thawing_date
  FROM
    trackback
    LEFT JOIN F4108 ON trackback.parent_batch_id = F4108.IOLOTN
    LEFT JOIN apds_lot ON trackback.cmpnt_batch_id = apds_lot.lot_number
    LEFT JOIN apds_lot_assign ON apds_lot.lot_pid = apds_lot_assign.lot_pid
    LEFT JOIN donordata ON apds_lot_assign.lot_entity_pid = donordata.unit_pid
    LEFT JOIN plasma_pool_thawing_date ON trackback.parent_batch_id = plasma_pool_thawing_date.plasma_batch_id
  GROUP BY
    batch_id, parent_batch_id, plasma_liters
  order BY parent_batch_id, batch_id ASC
),
sublotdata_with_row_number AS (
  SELECT
    sublotdata.*
    , ROW_NUMBER() OVER (PARTITION BY batch_id ORDER BY parent_batch_id) as row_num
  FROM sublotdata
),
summed_apds_data AS (
  SELECT
    batch_id
    , MAX(CASE WHEN row_num = 1 THEN parent_batch_id END) AS plasma_sublot1
    , MAX(CASE WHEN row_num = 2 THEN parent_batch_id END) AS plasma_sublot2
    , MAX(CASE WHEN row_num = 3 THEN parent_batch_id END) AS plasma_sublot3
    , SUM(plasma_volume_after_thawing) as plasma_volume_after_thawing
    , SUM(invoiced_plasma_volume) as invoiced_plasma_volume
    , MIN(thawing_date) as thawing_date
  FROM sublotdata_with_row_number
  GROUP BY batch_id
  order BY batch_id ASC
),
batch_id_without_y as (
  SELECT
  CASE 
    WHEN RIGHT(al_jde_data.batch_id, 1) = 'Y' THEN LEFT(al_jde_data.batch_id, LEN(al_jde_data.batch_id) - 1)
    ELSE al_jde_data.batch_id
  END AS batch_id
  , product_description
  , item_number
  , date_of_manufacture
  , date_of_expiration
  , CAST(completed_quantity AS STRING) as completed_quantity
  , plasma_sublot1
  , plasma_sublot2
  , plasma_sublot3
  , CAST(plasma_volume_after_thawing AS STRING) as plasma_volume_after_thawing
  , CAST(invoiced_plasma_volume AS STRING) as invoiced_plasma_volume
  , thawing_date
  FROM al_jde_data
  left join summed_apds_data on al_jde_data.batch_id = summed_apds_data.batch_id
)

select
  batch_id_without_y.batch_id
  , product_description
  , item_number
  , date_of_manufacture
  , date_of_expiration
  , completed_quantity
  , plasma_sublot1
  , plasma_sublot2
  , plasma_sublot3
  , plasma_volume_after_thawing
  , invoiced_plasma_volume
  , thawing_date
  , CAST(albumin AS STRING) as albumin
  , CASE 
      WHEN product_description IS NOT NULL 
        AND item_number IS NOT NULL 
        AND date_of_manufacture IS NOT NULL 
        AND date_of_expiration IS NOT NULL 
        AND completed_quantity IS NOT NULL 
        AND plasma_sublot1 IS NOT NULL 
        AND plasma_sublot2 IS NOT NULL 
        AND plasma_sublot3 IS NOT NULL 
        AND plasma_volume_after_thawing IS NOT NULL 
        AND invoiced_plasma_volume IS NOT NULL 
        AND thawing_date IS NOT NULL 
        AND albumin IS NOT NULL 
      THEN TRUE 
      ELSE FALSE 
    END AS all_data_available
  -- , CURRENT_TIMESTAMP AS ExtractionTimestamp
from batch_id_without_y
left join GLIMS ON batch_id_without_y.batch_id = GLIMS.batch_id
order BY batch_id ASC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3 plasmasublot combined

-- COMMAND ----------

-- Labware Data
with GLIMS as(
select
coalesce(mitigation_lotname.T_PH_LOT_NAME, lot_result.LOT_NUMBER) as batch_id -- merge LOT_ERP and LOTNUMBER_VALUE from both systems
, CAST(NUMBER_VALUE AS DECIMAL(10, 1)) AS Albumin
from gms_us_mart.txn_lot_result_approved_lims_glbl lot_result
left join gms_us_lake.gmsgq_glims_lot mitigation_lotname
ON lot_result.LOT = mitigation_lotname.LOT_NAME
where analysis like 'VN_PROTEIN_COMPOSI_ALBUMIN'
and testnamedetail like 'Albumin'
and lot_result.ACTIVE_FLAG like 'Y'
),
-- JDE
mb_filtered as (
  SELECT
  MATERIALBATCH_KEY
  , trim(BATCH_ID) as BATCH_ID
  , MAT_DESC
  , MAT_ID
  , BATCH_PROD_DT
  , SHELF_LIFE_EXPIRE_DT
  FROM gms_us_hub.ref_materialbatch_erp_glbl --JDE: F4108 'Lot Master'
),
f4108 as (
  SELECT 
  trim(IOLOTN) as BATCH_ID
  , CAST((IOUB04 / 10000) AS decimal(12, 4)) AS CompletedQuantity
  FROM gms_us_lake.gmsgq_jde_proddta_F4108_ADT
),
al_jde_data as (
  SELECT
  mb_filtered.BATCH_ID as BATCH_ID
  , mb_filtered.MAT_DESC as ProductDescription
  , mb_filtered.MAT_ID as ItemNumber
  , date_format(to_date(mb_filtered.BATCH_PROD_DT, 'yyyyMMdd'), 'dd-MMM-yyyy') AS DateofManufacture
  -- date_format(DATEADD(day, 365, to_date(mb_filtered.BATCH_PROD_DT, 'yyyyMMdd')), 'dd-MMM-yyyy') AS DateofExpiration,
  , date_format(to_date(mb_filtered.SHELF_LIFE_EXPIRE_DT, 'yyyyMMdd'), 'dd-MMM-yyyy') AS DateofExpiration
  , f4108.CompletedQuantity as CompletedQuantity
  FROM 
  mb_filtered
  left JOIN f4108 on trim(mb_filtered.BATCH_ID) = trim(f4108.BATCH_ID)
  WHERE
    mb_filtered.BATCH_ID like 'AL%'
  AND
    mb_filtered.BATCH_PROD_DT > 20240101
  AND
    f4108.CompletedQuantity > 0
),
-- JDE plasma pool thawing date
plasma_pool_thawing_date as (
  select
  trim(batch_id) as plasma_batch_id
  , to_date(created_on_dt, 'yyyyMMdd') as thawing_date -- Transaction Date = Thawing Date, is the same for all three plasma sublots
  from gms_us_hub.txn_stockmovement_erp_glbl
  where BATCH_ID like 'VIPL%'
  and mvmt_type_cd like 'IM'
),
-- APDS
F4108 AS(
  SELECT
    trim(IOLOTN) as IOLOTN
    , IOLDSC AS OriginDescription
    , CAST(LEFT(IOUB04, LEN(IOUB04) - 4) AS DECIMAL(10, 0)) AS PlasmaLiters
  FROM gms_us_lake.gmsgq_jde_proddta_F4108_ADT
),
trackback AS (
  SELECT distinct
    trim(root_batch_id) AS batch_id
    , trim(parent_batch_id) AS parent_batch_id
    , trim(cmpnt_batch_id) AS cmpnt_batch_id
  FROM gms_us_hub.ref_track_back_erp_glbl
  WHERE
    cmpnt_batch_id LIKE 'VIPL%'
    AND parent_batch_id LIKE 'VIPL%'
    AND root_batch_id LIKE 'AL%' -- ALK is alcohol and not albumin
    AND active_flag like 'Y'
),
apds_lot AS (
  SELECT DISTINCT
    lot_pid
    , lot_code
    , TRIM(lot_number) AS lot_number
    , schedule_number
  FROM gms_us_lake.gmsgqmi_apds_vn_dsp_lot
),
apds_lot_assign AS(
  SELECT distinct
    lot_pid
    , lot_entity_pid
  FROM gms_us_lake.gmsgqmi_apds_vn_pla_unit_lot_assign
),
donordata AS(
  SELECT distinct
    unit_pid
    , bleed_date
    , CAST(actual_volume AS DECIMAL(10, 3)) AS actual_volume
    , donor_number
    , disposition_action
  FROM
    gms_us_lake.gmsgqmi_apds_vn_pla_unit
  WHERE
    disposition_action NOT LIKE 'D'
),
sublotdata AS(
  SELECT distinct
    batch_id
    , parent_batch_id
    , PlasmaLiters as PlasmaVolumeAfterThawing
    , SUM(actual_volume) as InvoicedPlasmaVolume
    , date_format(MIN(thawing_date), 'dd-MMM-yyyy') as ThawingDate
  FROM
    trackback
    LEFT JOIN F4108 ON trackback.parent_batch_id = F4108.IOLOTN
    LEFT JOIN apds_lot ON trackback.cmpnt_batch_id = apds_lot.lot_number
    LEFT JOIN apds_lot_assign ON apds_lot.lot_pid = apds_lot_assign.lot_pid
    LEFT JOIN donordata ON apds_lot_assign.lot_entity_pid = donordata.unit_pid
    LEFT JOIN plasma_pool_thawing_date ON trackback.parent_batch_id = plasma_pool_thawing_date.plasma_batch_id
  GROUP BY
    BATCH_ID, parent_batch_id, PlasmaLiters
  order BY parent_batch_id, BATCH_ID ASC
),
summed_apds_data AS (
  SELECT
    BATCH_ID
    -- parent_batch_id,
    -- collect_list(parent_batch_id) AS PlasmaSublots,
    , collect_list(
      CASE 
        WHEN RIGHT(parent_batch_id, 1) = 'Y' THEN LEFT(parent_batch_id, LEN(parent_batch_id) - 1)
        ELSE parent_batch_id
      END
    ) AS PlasmaSublots
    , SUM(PlasmaVolumeAfterThawing) as PlasmaVolumeAfterThawing
    , SUM(InvoicedPlasmaVolume) as InvoicedPlasmaVolume
    , MIN(ThawingDate) as ThawingDate
    -- date_format(MIN(thawing_date), 'dd-MMM-yyyy') as thawing_date
  FROM sublotdata
  GROUP BY BATCH_ID
  order BY BATCH_ID ASC
),
batch_id_without_y as (
  SELECT
  CASE 
    WHEN RIGHT(al_jde_data.batch_id, 1) = 'Y' THEN LEFT(al_jde_data.batch_id, LEN(al_jde_data.batch_id) - 1)
    ELSE al_jde_data.batch_id
  END AS batch_id
  , ProductDescription
  , ItemNumber
  , DateofManufacture
  , DateofExpiration
  , CompletedQuantity
  , PlasmaSublots
  , PlasmaVolumeAfterThawing
  , InvoicedPlasmaVolume
  , ThawingDate
  FROM al_jde_data
  left join summed_apds_data on al_jde_data.BATCH_ID = summed_apds_data.BATCH_ID
)

select
  batch_id_without_y.batch_id
  , ProductDescription
  , ItemNumber
  , DateofManufacture
  , DateofExpiration
  , CompletedQuantity
  , PlasmaSublots
  , PlasmaVolumeAfterThawing
  , InvoicedPlasmaVolume
  , ThawingDate
  , Albumin
  , CASE 
      WHEN ProductDescription IS NOT NULL 
        AND ItemNumber IS NOT NULL 
        AND DateofManufacture IS NOT NULL 
        AND DateofExpiration IS NOT NULL 
        AND CompletedQuantity IS NOT NULL 
        AND PlasmaSublots IS NOT NULL 
        AND PlasmaVolumeAfterThawing IS NOT NULL 
        AND InvoicedPlasmaVolume IS NOT NULL 
        AND ThawingDate IS NOT NULL 
        AND Albumin IS NOT NULL 
      THEN TRUE 
      ELSE FALSE 
    END AS AllDataAvailable
  -- , CURRENT_TIMESTAMP AS ExtractionTimestamp
from batch_id_without_y
left join GLIMS ON batch_id_without_y.BATCH_ID = GLIMS.BATCH_ID
order BY batch_id ASC


-- COMMAND ----------

-- MAGIC %md
-- MAGIC # sources
-- MAGIC In the following cells the data sources are seperated for maintanance and troubleshooting with the data model
-- MAGIC - not part of the hackathon, but probably interesting

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## jde

-- COMMAND ----------

WITH
mb_filtered as (
  SELECT
  MATERIALBATCH_KEY,
  trim(BATCH_ID) as BATCH_ID,
  MAT_DESC,
  MAT_ID,
  BATCH_PROD_DT,
  SHELF_LIFE_EXPIRE_DT
  FROM gms_us_hub.ref_materialbatch_erp_glbl --JDE: F4108 "Lot Master"
),
f4108 as (
  SELECT 
  trim(IOLOTN) as BATCH_ID,
  CAST((IOUB04 / 10000) AS decimal(12, 4)) AS CompletedQuantity
  FROM gms_us_lake.gmsgq_jde_proddta_F4108_ADT
),
qa_vie_precg_jde_vw as (
  SELECT
  mb_filtered.BATCH_ID as BATCH_ID,
  mb_filtered.MAT_DESC as ProductDescription,
  mb_filtered.MAT_ID as ItemNumber,
  date_format(to_date(mb_filtered.BATCH_PROD_DT, 'yyyyMMdd'), 'dd-MMM-yyyy') AS DateofManufacture,
  -- date_format(DATEADD(day, 365, to_date(mb_filtered.BATCH_PROD_DT, 'yyyyMMdd')), 'dd-MMM-yyyy') AS DateofExpiration,
  date_format(to_date(mb_filtered.SHELF_LIFE_EXPIRE_DT, 'yyyyMMdd'), 'dd-MMM-yyyy') AS DateofExpiration,
  f4108.CompletedQuantity as CompletedQuantity
  FROM 
  mb_filtered
  left JOIN f4108 on trim(mb_filtered.BATCH_ID) = trim(f4108.BATCH_ID)
  WHERE
    mb_filtered.BATCH_ID like 'AL%'
  AND
    mb_filtered.BATCH_PROD_DT > 20240101
  AND
    f4108.CompletedQuantity > 0
)
-- SELECT * from qa_vie_precg_jde_vw
-- where BATCH_ID like 'ALB0433'
-- where BATCH_ID like 'ALB0426'
SELECT * from mb_filtered
where BATCH_ID like 'ALB0433'

-- COMMAND ----------

cmpnt_mvmt_type_cd

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## plasma pool thawing date

-- COMMAND ----------

select
batch_id,
to_date(created_on_dt, 'yyyyMMdd') as created_on_dt -- Transaction Date = Thawing Date, is the same for all three plasma sublots
-- batch_mfg_dt -- Production Date, not relevant
from gms_us_hub.txn_stockmovement_erp_glbl
-- where BATCH_ID like 'ALB0433%'
where BATCH_ID like 'VIPLB0150A%'
and mvmt_type_cd like 'IM'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## plasma data

-- COMMAND ----------

WITH
F4108 AS(
  SELECT
    trim(IOLOTN) as IOLOTN,
    IOLDSC AS OriginDescription,
    CAST(LEFT(IOUB04, LEN(IOUB04) - 4) AS DECIMAL(10, 0)) AS PlasmaLiters
  FROM gms_us_lake.gmsgq_jde_proddta_F4108_ADT
),
trackback AS (
  SELECT distinct
    root_batch_id,
    trim(parent_batch_id) AS parent_batch_id,
    trim(cmpnt_batch_id) AS cmpnt_batch_id
  FROM gms_us_hub.ref_track_back_erp_glbl
),
apds_lot AS (
  SELECT DISTINCT
    lot_pid,
    lot_code,
    TRIM(lot_number) AS lot_number,
    schedule_number
  FROM gms_us_lake.gmsgqmi_apds_vn_dsp_lot
),
apds_lot_assign AS(
  SELECT distinct
    lot_pid,
    lot_entity_pid
  FROM gms_us_lake.gmsgqmi_apds_vn_pla_unit_lot_assign
),
donordata AS(
  SELECT distinct
    unit_pid,
    bleed_date,
    CAST(actual_volume AS DECIMAL(10, 3)) AS actual_volume,
    donor_number,
    disposition_action
  FROM
    gms_us_lake.gmsgqmi_apds_vn_pla_unit
  WHERE
    disposition_action NOT LIKE 'D'
),
sublotdata AS(
  SELECT distinct
    trim(root_batch_id) as BATCH_ID,
    parent_batch_id,
    PlasmaLiters as PlasmaVolumeAfterThawing,
    SUM(actual_volume) as InvoicedPlasmaVolume
  FROM
    trackback
    LEFT JOIN F4108 ON trackback.parent_batch_id = F4108.IOLOTN
    LEFT JOIN apds_lot ON trackback.cmpnt_batch_id = apds_lot.lot_number
    LEFT JOIN apds_lot_assign ON apds_lot.lot_pid = apds_lot_assign.lot_pid
    LEFT JOIN donordata ON apds_lot_assign.lot_entity_pid = donordata.unit_pid
  WHERE
    trackback.cmpnt_batch_id LIKE "VIPL%"
    AND trackback.parent_batch_id LIKE "VIPL%"
    AND trackback.root_batch_id LIKE "AL%"
  GROUP BY
    BATCH_ID, parent_batch_id, PlasmaLiters
  order BY parent_batch_id, BATCH_ID ASC
),
summed_data AS (
  SELECT
    BATCH_ID,
    -- parent_batch_id,
    collect_list(parent_batch_id) AS parent_batch_ids,
    SUM(PlasmaVolumeAfterThawing),
    SUM(InvoicedPlasmaVolume)
  FROM sublotdata
  GROUP BY BATCH_ID
  order BY BATCH_ID ASC
)

SELECT * FROM summed_data

-- COMMAND ----------


WITH
F4108 AS(
  SELECT
    trim(IOLOTN) as IOLOTN,
    IOLDSC AS OriginDescription,
    CAST(LEFT(IOUB04, LEN(IOUB04) - 4) AS DECIMAL(10, 0)) AS PlasmaLiters -- no comma no problem
    -- LEFT(IOUB04, LEN(IOUB04) - 4) AS PlasmaLiters -- alternative
    -- IOUB04 / 10000 as PlasmaLiters
  FROM gms_us_lake.gmsgq_jde_proddta_F4108_ADT
),
trackback AS (
  SELECT distinct
    -- *,
    root_batch_id,
    trim(parent_batch_id) AS parent_batch_id,
    trim(cmpnt_batch_id) AS cmpnt_batch_id
  FROM gms_us_hub.ref_track_back_erp_glbl
  where active_flag like 'Y'
),
apds_lot AS (
  SELECT DISTINCT
    lot_pid,
    lot_code,
    TRIM(lot_number) AS lot_number,
    schedule_number
  FROM gms_us_lake.gmsgqmi_apds_vn_dsp_lot
),
-- The lot_pid can be used to identify the lot_entity_pid with the table gmsgqmi_apds_vn_pla_unit_lot_assign
apds_lot_assign AS(
  SELECT distinct
    lot_pid,
    lot_entity_pid
  FROM gms_us_lake.gmsgqmi_apds_vn_pla_unit_lot_assign
),
donordata AS(
  SELECT distinct
    unit_pid,
    bleed_date,
    expected_volume,
    actual_volume,
    donor_number,
    disposition_action
    FROM
    gms_us_lake.gmsgqmi_apds_vn_pla_unit
    WHERE
    disposition_action NOT LIKE 'D'
),

sublotdata AS(
SELECT distinct
  trim(root_batch_id) as BATCH_ID,
  parent_batch_id,
  PlasmaLiters as PlasmaVolumeAfterThawing,
  SUM(actual_volume) as InvoicedPlasmaVolume
  -- COUNT(donor_number) AS sublot_donor_number, -- Donations / Bleeds
  -- COUNT(DISTINCT donor_number) AS sublot_unique_donor_number -- Donors
FROM
  trackback
  LEFT JOIN F4108 ON trackback.parent_batch_id = F4108.IOLOTN
  LEFT JOIN apds_lot ON trackback.cmpnt_batch_id = apds_lot.lot_number
  LEFT JOIN apds_lot_assign ON apds_lot.lot_pid = apds_lot_assign.lot_pid
  LEFT JOIN donordata ON apds_lot_assign.lot_entity_pid = donordata.unit_pid
WHERE
  trackback.cmpnt_batch_id LIKE "VIPL%"
  AND trackback.parent_batch_id LIKE "VIPL%"
  AND trackback.root_batch_id LIKE "AL%"
GROUP BY
  BATCH_ID, parent_batch_id, PlasmaLiters
ORDER BY parent_batch_id, BATCH_ID ASC
)

SELECT * from sublotdata

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## labware

-- COMMAND ----------


with GLIMS as(

select
coalesce(mitigation_lotname.T_PH_LOT_NAME, lot_result.LOT_NUMBER) as LOT_NUMBER -- merge LOT_ERP and LOTNUMBER_VALUE from both systems
-- lot_result.LOT_NUMBER,
-- , LOT
-- , TEXT_VALUE
-- , NUMBER_VALUE
, CAST(NUMBER_VALUE AS DECIMAL(10, 1)) AS Albumin
-- , operation
-- , component
-- , analysis
-- , method_datagroup
-- , measure
-- , testnamedetail
-- , testnamegeneral
-- , productfamily
-- , result_condition
-- , lot_result.ACTIVE_FLAG as active_flag
from gms_us_mart.txn_lot_result_approved_lims_glbl lot_result
left join gms_us_lake.gmsgq_glims_lot mitigation_lotname
ON lot_result.LOT = mitigation_lotname.LOT_NAME
where analysis like 'VN_PROTEIN_COMPOSI_ALBUMIN'
and testnamedetail like 'Albumin'
and lot_result.ACTIVE_FLAG like 'Y'
)

select
*
from GLIMS
-- where GLIMS.LOT_NUMBER like "ALB0592%"
where GLIMS.LOT_NUMBER like "ALB0433%"



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## veeva
-- MAGIC -- not integrated in data model

-- COMMAND ----------

-- Veeva Metadata⁠, example:
select
title__v as DocumentName,
concat(major_version_number__v, '.', minor_version_number__v) as Version
from gms_us_lake.gmsgq_veeva_documents
WHERE document_number__v like 'FORM-304391'
and status__v like 'Effective'
and latest_version__v like 'true'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # table creation

-- COMMAND ----------

-- Labware Data
with GLIMS as(
select
coalesce(mitigation_lotname.T_PH_LOT_NAME, lot_result.LOT_NUMBER) as batch_id -- merge LOT_ERP and LOTNUMBER_VALUE from both systems
, CAST(NUMBER_VALUE AS DECIMAL(10, 1)) AS Albumin
from gms_us_mart.txn_lot_result_approved_lims_glbl lot_result
left join gms_us_lake.gmsgq_glims_lot mitigation_lotname
ON lot_result.LOT = mitigation_lotname.LOT_NAME
where analysis like 'VN_PROTEIN_COMPOSI_ALBUMIN'
and testnamedetail like 'Albumin'
and lot_result.ACTIVE_FLAG like 'Y'
),
-- JDE
mb_filtered as (
  SELECT
  MATERIALBATCH_KEY
  , trim(BATCH_ID) as BATCH_ID
  , MAT_DESC
  , MAT_ID
  , BATCH_PROD_DT
  , SHELF_LIFE_EXPIRE_DT
  FROM gms_us_hub.ref_materialbatch_erp_glbl --JDE: F4108 "Lot Master"
),
f4108 as (
  SELECT 
  trim(IOLOTN) as BATCH_ID
  , CAST((IOUB04 / 10000) AS decimal(12, 4)) AS CompletedQuantity
  FROM gms_us_lake.gmsgq_jde_proddta_F4108_ADT
),
al_jde_data as (
  SELECT
  mb_filtered.BATCH_ID as BATCH_ID
  , mb_filtered.MAT_DESC as ProductDescription
  , mb_filtered.MAT_ID as ItemNumber
  , date_format(to_date(mb_filtered.BATCH_PROD_DT, 'yyyyMMdd'), 'dd-MMM-yyyy') AS DateofManufacture
  -- date_format(DATEADD(day, 365, to_date(mb_filtered.BATCH_PROD_DT, 'yyyyMMdd')), 'dd-MMM-yyyy') AS DateofExpiration,
  , date_format(to_date(mb_filtered.SHELF_LIFE_EXPIRE_DT, 'yyyyMMdd'), 'dd-MMM-yyyy') AS DateofExpiration
  , f4108.CompletedQuantity as CompletedQuantity
  FROM 
  mb_filtered
  left JOIN f4108 on trim(mb_filtered.BATCH_ID) = trim(f4108.BATCH_ID)
  WHERE
    mb_filtered.BATCH_ID like 'AL%'
  AND
    mb_filtered.BATCH_PROD_DT > 20240101
  AND
    f4108.CompletedQuantity > 0
),
-- JDE plasma pool thawing date
plasma_pool_thawing_date as (
  select
  trim(batch_id) as plasma_batch_id
  , to_date(created_on_dt, 'yyyyMMdd') as thawing_date -- Transaction Date = Thawing Date, is the same for all three plasma sublots
  from gms_us_hub.txn_stockmovement_erp_glbl
  where BATCH_ID like 'VIPL%'
  and mvmt_type_cd like 'IM'
),
-- APDS
F4108 AS(
  SELECT
    trim(IOLOTN) as IOLOTN
    , IOLDSC AS OriginDescription
    , CAST(LEFT(IOUB04, LEN(IOUB04) - 4) AS DECIMAL(10, 0)) AS PlasmaLiters
  FROM gms_us_lake.gmsgq_jde_proddta_F4108_ADT
),
trackback AS (
  SELECT distinct
    trim(root_batch_id) AS batch_id
    , trim(parent_batch_id) AS parent_batch_id
    , trim(cmpnt_batch_id) AS cmpnt_batch_id
  FROM gms_us_hub.ref_track_back_erp_glbl
  WHERE
    cmpnt_batch_id LIKE "VIPL%"
    AND parent_batch_id LIKE "VIPL%"
    AND root_batch_id LIKE 'AL%' -- ALK is alcohol and not albumin
    AND active_flag like 'Y'
),
apds_lot AS (
  SELECT DISTINCT
    lot_pid
    , lot_code
    , TRIM(lot_number) AS lot_number
    , schedule_number
  FROM gms_us_lake.gmsgqmi_apds_vn_dsp_lot
),
apds_lot_assign AS(
  SELECT distinct
    lot_pid
    , lot_entity_pid
  FROM gms_us_lake.gmsgqmi_apds_vn_pla_unit_lot_assign
),
donordata AS(
  SELECT distinct
    unit_pid
    , bleed_date
    , CAST(actual_volume AS DECIMAL(10, 3)) AS actual_volume
    , donor_number
    , disposition_action
  FROM
    gms_us_lake.gmsgqmi_apds_vn_pla_unit
  WHERE
    disposition_action NOT LIKE 'D'
),
sublotdata AS(
  SELECT distinct
    batch_id
    , parent_batch_id
    , PlasmaLiters as PlasmaVolumeAfterThawing
    , SUM(actual_volume) as InvoicedPlasmaVolume
    , date_format(MIN(thawing_date), 'dd-MMM-yyyy') as ThawingDate
  FROM
    trackback
    LEFT JOIN F4108 ON trackback.parent_batch_id = F4108.IOLOTN
    LEFT JOIN apds_lot ON trackback.cmpnt_batch_id = apds_lot.lot_number
    LEFT JOIN apds_lot_assign ON apds_lot.lot_pid = apds_lot_assign.lot_pid
    LEFT JOIN donordata ON apds_lot_assign.lot_entity_pid = donordata.unit_pid
    LEFT JOIN plasma_pool_thawing_date ON trackback.parent_batch_id = plasma_pool_thawing_date.plasma_batch_id
  GROUP BY
    BATCH_ID, parent_batch_id, PlasmaLiters
  order BY parent_batch_id, BATCH_ID ASC
),
summed_apds_data AS (
  SELECT
    BATCH_ID
    -- parent_batch_id,
    -- collect_list(parent_batch_id) AS PlasmaSublots,
    , collect_list(
      CASE 
        WHEN RIGHT(parent_batch_id, 1) = 'Y' THEN LEFT(parent_batch_id, LEN(parent_batch_id) - 1)
        ELSE parent_batch_id
      END
    ) AS PlasmaSublots
    , SUM(PlasmaVolumeAfterThawing) as PlasmaVolumeAfterThawing
    , SUM(InvoicedPlasmaVolume) as InvoicedPlasmaVolume
    , MIN(ThawingDate) as ThawingDate
    -- date_format(MIN(thawing_date), 'dd-MMM-yyyy') as thawing_date
  FROM sublotdata
  GROUP BY BATCH_ID
  order BY BATCH_ID ASC
),
batch_id_without_y as (
  SELECT
  CASE 
    WHEN RIGHT(al_jde_data.batch_id, 1) = 'Y' THEN LEFT(al_jde_data.batch_id, LEN(al_jde_data.batch_id) - 1)
    ELSE al_jde_data.batch_id
  END AS batch_id
  , ProductDescription
  , ItemNumber
  , DateofManufacture
  , DateofExpiration
  , CompletedQuantity
  , PlasmaSublots
  , PlasmaVolumeAfterThawing
  , InvoicedPlasmaVolume
  , ThawingDate
  FROM al_jde_data
  left join summed_apds_data on al_jde_data.BATCH_ID = summed_apds_data.BATCH_ID
)



CREATE TABLE gms_us_alyt.vie_hackathon AS
select * from (
  select
    batch_id_without_y.batch_id
    , ProductDescription
    , ItemNumber
    , DateofManufacture
    , DateofExpiration
    , CompletedQuantity
    , PlasmaSublots
    , PlasmaVolumeAfterThawing
    , InvoicedPlasmaVolume
    , ThawingDate
    , Albumin
    , CASE 
        WHEN ProductDescription IS NOT NULL 
          AND ItemNumber IS NOT NULL 
          AND DateofManufacture IS NOT NULL 
          AND DateofExpiration IS NOT NULL 
          AND CompletedQuantity IS NOT NULL 
          AND PlasmaSublots IS NOT NULL 
          AND PlasmaVolumeAfterThawing IS NOT NULL 
          AND InvoicedPlasmaVolume IS NOT NULL 
          AND ThawingDate IS NOT NULL 
          AND Albumin IS NOT NULL 
        THEN TRUE 
        ELSE FALSE 
      END AS AllDataAvailable
    -- , CURRENT_TIMESTAMP AS ExtractionTimestamp
  from batch_id_without_y
  left join GLIMS ON batch_id_without_y.BATCH_ID = GLIMS.BATCH_ID
  order BY batch_id ASC
) t
