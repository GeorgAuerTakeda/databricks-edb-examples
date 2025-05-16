-- Databricks notebook source
with
country_affected as (
select
  pr_id,
  pr_addtl_data_id,
  max(case when data_field_nm = 'RA - Product Family' then field_value end) as RA_Product_Family,
  max(case when data_field_nm = 'Market' then field_value end) as Market,
  max(case when data_field_nm = 'Filing Type Supporting Change' then field_value end) as Filing_Type_Supporting_Change,
  max(case when data_field_nm = 'Is Prior Approval Required' then field_value end) as Is_Prior_Approval_Required,
  max(case when data_field_nm = 'Local Prod. Labeling Impact?' then field_value end) as Local_Prod_Labeling_Impact,
  max(case when data_field_nm = 'RA - Supply Distribution Date' then field_value end) as RA_Supply_Distribution_Date,
  max(
    case 
      when data_field_nm = 'Market' and field_value is not null then pr_addtl_data_id - 1
      when data_field_nm = 'Filing Type Supporting Change' and field_value is not null then pr_addtl_data_id - 2
      when data_field_nm = 'Is Prior Approval Required' and field_value is not null then pr_addtl_data_id - 3
      when data_field_nm = 'Local Prod. Labeling Impact?' and field_value is not null then pr_addtl_data_id - 4
      when data_field_nm = 'RA - Supply Distribution Date' and field_value is not null then pr_addtl_data_id - 5
      else pr_addtl_data_id 
  end) as pr_addtl_data_id_adjusted
from gms_us_mart.txn_pr_addtl_data_detail_trkw_glbl
where pr_id like '4097013%'
group by pr_id, pr_addtl_data_id
order by pr_id, pr_addtl_data_id
),

country_affected_filtered as (
select
  pr_id,
  MAX(RA_Product_Family) as RA_Product_Family,
  MAX(Market) as Market,
  MAX(Filing_Type_Supporting_Change) as Filing_Type_Supporting_Change,
  MAX(Is_Prior_Approval_Required) as Is_Prior_Approval_Required,
  MAX(Local_Prod_Labeling_Impact) as Local_Prod_Labeling_Impact,
  MAX(RA_Supply_Distribution_Date) as RA_Supply_Distribution_Date
from country_affected
group by pr_id, pr_addtl_data_id_adjusted
)

select *
from country_affected_filtered
where RA_Product_Family is not null


-- COMMAND ----------

with
country_affected as (
select
  pr_id,
  pr_addtl_data_id,
  max(case when data_field_nm = 'RA - Product Family' then field_value end) as RA_Product_Family,
  max(case when data_field_nm = 'Market' then field_value end) as Market,
  max(case when data_field_nm = 'Filing Type Supporting Change' then field_value end) as Filing_Type_Supporting_Change,
  max(
    case 
      when data_field_nm = 'Market' and field_value is not null then pr_addtl_data_id - 1
      when data_field_nm = 'Filing Type Supporting Change' and field_value is not null then pr_addtl_data_id - 2
      else pr_addtl_data_id 
  end) as pr_addtl_data_id_adjusted
from gms_us_mart.txn_pr_addtl_data_detail_trkw_glbl
where pr_id like '4097013%'
group by pr_id, pr_addtl_data_id
order by pr_id, pr_addtl_data_id
)
-- RA - Product Family
-- Market
-- Filing Type Supporting Change
-- Is Prior Approval Required
-- Local Prod. Labeling Impact?
-- RA - Supply Distribution Date
-- RA - Product Family

select
  pr_id
  ,MAX(RA_Product_Family) as RA_Product_Family
  ,MAX(Market) as Market
  ,MAX(Filing_Type_Supporting_Change) as Filing_Type_Supporting_Change
from country_affected
group by pr_id, pr_addtl_data_id_adjusted

-- COMMAND ----------

with
country_affected as (
select
  pr_addtl_data_id
  ,data_field_nm
  -- max(case when data_field_nm = 'Market' then field_value end) as Market,
  -- max(case when data_field_nm = 'Filing Type Supporting Change' then field_value end) as Filing_Type_Supporting_Change,
  -- max(case when data_field_nm = 'Filing Type Supporting Change' and field_value is not null then pr_addtl_data_id - 1 else pr_addtl_data_id end) as pr_addtl_data_id_adjusted
  ,*
from gms_us_mart.txn_pr_addtl_data_detail_trkw_glbl
where pr_id like '4097013%'
order by pr_addtl_data_id
)

select * from country_affected

-- COMMAND ----------

select
LOT
,MATERIAL_DESC
,RECOMMENDED_DISPO
,DISPOSITION_DATE
,PROJ_NM
,STAT_NM
,COMMENT
,OPERATIONAL_UNIT
,OWNING_GROUP
,SEQ_NO
,*
 from gms_us_mart.txn_grid_affected_material_and_lot_trkw_glbl
 where proj_nm like '%RA Assessment%'
--  where material_desc like 'FIBRIN%'
-- where LOT like 'ANB308' --Lab Investigation
-- or LOT like 'ANB334A' --Lab Investigation
-- or LOT like 'K5A035S1' --Deviation
-- or LOT like 'K5A035' --Deviation

-- COMMAND ----------


select
data_field_nm
,field_value
,data_field_id
,pr_addtl_data_id
,* from gms_us_mart.txn_pr_addtl_data_detail_trkw_glbl
 where 1 = 1
--  and(data_field_nm like 'Market'
--  or data_field_nm like 'Filing Type Supporting Change'
--  or data_field_nm like 'Is Prior Approval Required'
--  or data_field_nm like 'RA - Product Family')
 and pr_id like '4097013%'
 sort by pr_addtl_data_id

-- COMMAND ----------

select * from gms_us_mart.ref_pr_detail_trkw_glbl
 where pr_id like '4097013%'

-- COMMAND ----------

select * from gms_us_hub.txn_pr_trkw_glbl
 where pr_id like '4097013%'
