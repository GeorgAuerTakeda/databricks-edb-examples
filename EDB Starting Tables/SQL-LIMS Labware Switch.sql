-- Databricks notebook source
with GLIMS as(
select
LOT
, coalesce(mitigation_lotname.T_PH_LOT_NAME, lot_result.LOT_NUMBER) as LOT_NUMBER -- merge LOT_ERP and LOTNUMBER_VALUE from both systems
, lot_result.LOT
, TEXT_VALUE
, NUMBER_VALUE
, operation
, component
, method_datagroup
, measure
, testnamedetail
, result_condition
, lot_result.ACTIVE_FLAG as active_flag
, SITE_NM
, lot_result.src_system
, date_trunc('year', lot_result.AUD_LD_DTS) as AUD_LD_DTS
-- , case when month(lot_result.AUD_LD_DTS) <= 6 then date_trunc('year', lot_result.AUD_LD_DTS) else date_add(date_trunc('year', lot_result.AUD_LD_DTS), 182) end as AUD_LD_DTS
, substring(coalesce(mitigation_lotname.T_PH_LOT_NAME, lot_result.LOT_NUMBER), 1, 2) as lot_nr_prefix
-- , substring(lot_result.LOT, 1, 2) as lot_prefix
from gms_us_mart.txn_lot_result_approved_lims_glbl lot_result
left join gms_us_lake.gmsgq_glims_lot mitigation_lotname
ON lot_result.LOT = mitigation_lotname.LOT_NAME
where (site_nm like 'Central Europe' or site_nm like 'Vienna')
and substring(coalesce(mitigation_lotname.T_PH_LOT_NAME, lot_result.LOT_NUMBER), 1, 2) not rlike '^[0-9]+$' -- exclude lots that do not have letters as first two chars
)

select
SITE_NM
, lot_nr_prefix
, src_system
, AUD_LD_DTS
from GLIMS
order by lot_nr_prefix, AUD_LD_DTS

-- COMMAND ----------

with GLIMS as(
select
LOT
, coalesce(mitigation_lotname.T_PH_LOT_NAME, lot_result.LOT_NUMBER) as LOT_NUMBER -- merge LOT_ERP and LOTNUMBER_VALUE from both systems
, lot_result.LOT
, TEXT_VALUE
, NUMBER_VALUE
, operation
, component
, method_datagroup
, measure
, testnamedetail
, result_condition
, lot_result.ACTIVE_FLAG as active_flag
, SITE_NM
, lot_result.src_system
, lot_result.AUD_LD_DTS
, substring(coalesce(mitigation_lotname.T_PH_LOT_NAME, lot_result.LOT_NUMBER), 1, 2) as lot_nr_prefix
-- , substring(lot_result.LOT, 1, 2) as lot_prefix
from gms_us_mart.txn_lot_result_approved_lims_glbl lot_result
left join gms_us_lake.gmsgq_glims_lot mitigation_lotname
ON lot_result.LOT = mitigation_lotname.LOT_NAME
where (site_nm like 'Central Europe' or site_nm like 'Vienna')
and substring(coalesce(mitigation_lotname.T_PH_LOT_NAME, lot_result.LOT_NUMBER), 1, 2) not rlike '^[0-9]+$' -- exclude lots that do not have letters as first char
)

select distinct
-- LOT_NUMBER
SITE_NM
-- ,lot_prefix
,lot_nr_prefix
,src_system
,MAX(AUD_LD_DTS) as last_date
from GLIMS
group by SITE_NM, lot_nr_prefix, src_system having MAX(AUD_LD_DTS) > '2025-01-01T01:00:00.000+00:00'
and MAX(AUD_LD_DTS) > '2025-01-01T01:00:00.000+00:00'
-- group by SITE_NM,lot_prefix, lot_nr_prefix, src_system

-- COMMAND ----------

select * from gms_us_mart.txn_lot_result_approved_lims_glbl

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #bulk

-- COMMAND ----------

with GLIMS as (
    select
        LOT,
        coalesce(mitigation_lotname.T_PH_LOT_NAME, lot_result.LOT_NUMBER) as LOT_NUMBER, -- merge LOT_ERP and LOTNUMBER_VALUE from both systems
        lot_result.LOT,
        TEXT_VALUE,
        NUMBER_VALUE,
        operation,
        component,
        method_datagroup,
        measure,
        testnamedetail,
        result_condition,
        lot_result.ACTIVE_FLAG as active_flag,
        SITE_NM,
        lot_result.src_system,
        lot_result.AUD_LD_DTS as AUD_LD_DTS,
-- , substring(coalesce(mitigation_lotname.T_PH_LOT_NAME, lot_result.LOT_NUMBER), 1, 2) as lot_nr_prefix
-- , case when coalesce(mitigation_lotname.T_PH_LOT_NAME, lot_result.LOT_NUMBER) like 'RI%' then substring(coalesce(mitigation_lotname.T_PH_LOT_NAME, lot_result.LOT_NUMBER), 1, 4) 
-- ,case when coalesce(mitigation_lotname.T_PH_LOT_NAME, lot_result.LOT_NUMBER) like 'PY%' then substring(coalesce(mitigation_lotname.T_PH_LOT_NAME, lot_result.LOT_NUMBER), 1, 4) else substring(coalesce(mitigation_lotname.T_PH_LOT_NAME, lot_result.LOT_NUMBER), 1, 2) end as lot_nr_prefix
        case 
            when coalesce(mitigation_lotname.T_PH_LOT_NAME, lot_result.LOT_NUMBER) like 'RI%' then substring(coalesce(mitigation_lotname.T_PH_LOT_NAME, lot_result.LOT_NUMBER), 1, 6)
            when coalesce(mitigation_lotname.T_PH_LOT_NAME, lot_result.LOT_NUMBER) like 'PY%' then substring(coalesce(mitigation_lotname.T_PH_LOT_NAME, lot_result.LOT_NUMBER), 1, 4)
            else substring(coalesce(mitigation_lotname.T_PH_LOT_NAME, lot_result.LOT_NUMBER), 1, 2)
        end as lot_nr_prefix
    from gms_us_mart.txn_lot_result_approved_lims_glbl lot_result
    left join gms_us_lake.gmsgq_glims_lot mitigation_lotname
    ON lot_result.LOT = mitigation_lotname.LOT_NAME
    where (site_nm like 'Central Europe' or site_nm like 'Vienna')
    and substring(coalesce(mitigation_lotname.T_PH_LOT_NAME, lot_result.LOT_NUMBER), 1, 2) not rlike '^[0-9]+$' -- exclude lots that do not have letters as first char
    and lot_result.AUD_LD_DTS > '2022-01-01T01:00:00.000+00:00'
)

select distinct
    lot_nr_prefix,
    src_system,
    MAX(AUD_LD_DTS) as last_date,
    SITE_NM
from GLIMS
where (
    lot_number LIKE 'AN%' OR
    lot_number LIKE 'AL%' OR
    lot_number LIKE 'EL%' OR
    lot_number LIKE 'FN%' OR
    lot_number LIKE 'FE%' OR
    lot_number LIKE 'FA%' OR
    lot_number LIKE 'LS%' OR
    lot_number LIKE 'LR%' OR
    lot_number LIKE 'CF%' OR
    lot_number LIKE 'CG%' OR
    lot_number LIKE 'CE%' OR
    lot_number LIKE 'DK%' OR
    lot_number LIKE 'BF%' OR
    lot_number LIKE 'BB%' OR
    lot_number LIKE 'BK%' OR
    lot_number LIKE 'BH%' OR
    lot_number LIKE 'BQ%' OR
    lot_number LIKE 'BP%' OR
    lot_number LIKE 'BN%' OR
    lot_number LIKE 'CB%' OR
    lot_number LIKE 'KE%' OR
    lot_number LIKE 'CA%' OR
    lot_number LIKE 'DD%' OR
    lot_number LIKE 'DA%' OR
    lot_number LIKE 'DN%' OR
    lot_number LIKE 'PL%' OR
    lot_number LIKE 'LP%' OR
    lot_number LIKE 'RI%' OR
    lot_number LIKE 'PY%' OR
    lot_number LIKE 'LB%'
)
group by SITE_NM, lot_nr_prefix, src_system

-- COMMAND ----------

select 
-- lims_lot.t_ph_lot_name as batch_id,
-- case when count(distinct status) > 1 then false else max(case when status = 'A' then true else false end) end as check_status
src_system
-- ,*
,substring(lot, 1, 2) as lot_prefix
,substring(lot_number, 1, 2) as lot_number_prefix
from gms_us_mart.txn_lot_result_approved_lims_glbl
-- where lot_number LIKE 'PL%'
where AUD_LD_DTS > '2024-11-01T19:25:15.445+00:00'
AND (
    lot_number LIKE 'AN%' OR
    lot_number LIKE 'AL%' OR
    lot_number LIKE 'EL%' OR
    lot_number LIKE 'FN%' OR
    lot_number LIKE 'FE%' OR
    lot_number LIKE 'FA%' OR
    lot_number LIKE 'LS%' OR
    lot_number LIKE 'LR%' OR
    lot_number LIKE 'CF%' OR
    lot_number LIKE 'CG%' OR
    lot_number LIKE 'CE%' OR
    lot_number LIKE 'DK%' OR
    lot_number LIKE 'BF%' OR
    lot_number LIKE 'BB%' OR
    lot_number LIKE 'BK%' OR
    lot_number LIKE 'BH%' OR
    lot_number LIKE 'BQ%' OR
    lot_number LIKE 'BP%' OR
    lot_number LIKE 'BN%' OR
    lot_number LIKE 'CB%' OR
    lot_number LIKE 'KE%' OR
    lot_number LIKE 'CA%' OR
    lot_number LIKE 'DD%' OR
    lot_number LIKE 'DA%' OR
    lot_number LIKE 'DN%' OR
    lot_number LIKE 'PL%' OR
    lot_number LIKE 'LP%' OR
    lot_number LIKE 'RI%' OR
    lot_number LIKE 'PY%' OR
    lot_number LIKE 'LB%'
)
or ((AUD_LD_DTS > '2024-11-01T19:25:15.445+00:00') AND ( lot LIKE 'AN%' OR
      lot LIKE 'AL%' OR
      lot LIKE 'EL%' OR
      lot LIKE 'FN%' OR
      lot LIKE 'FE%' OR
      lot LIKE 'FA%' OR
      lot LIKE 'LS%' OR
      lot LIKE 'LR%' OR
      lot LIKE 'CF%' OR
      lot LIKE 'CG%' OR
      lot LIKE 'CE%' OR
      lot LIKE 'DK%' OR
      lot LIKE 'BF%' OR
      lot LIKE 'BB%' OR
      lot LIKE 'BK%' OR
      lot LIKE 'BH%' OR
      lot LIKE 'BQ%' OR
      lot LIKE 'BP%' OR
      lot LIKE 'BN%' OR
      lot LIKE 'CB%' OR
      lot LIKE 'KE%' OR
      lot LIKE 'CA%' OR
      lot LIKE 'DD%' OR
      lot LIKE 'DA%' OR
      lot LIKE 'DN%' OR
      lot LIKE 'PL%' OR
      lot LIKE 'LP%' OR
      lot LIKE 'RI%' OR
      lot LIKE 'PY%' OR
      lot LIKE 'LB%'))
group by lot_prefix, lot_number_prefix, src_system

-- COMMAND ----------

select 
-- lims_lot.t_ph_lot_name as batch_id,
case when count(distinct status) > 1 then false else max(case when status = 'A' then true else false end) end as check_status,
lims_lot.aud_src_sys_id as src_sys,
substring(lims_lot.t_ph_lot_name, 1, 2) as lot_prefix
from gms_us_lake.gmsgq_glims_sample lims_sample
left join gms_us_lake.gmsgq_glims_lot lims_lot
on lims_sample.lot = lims_lot.lot_number
where text_id not like '%QA_DISPO%'
and ( t_ph_lot_name LIKE 'AN%' OR
      t_ph_lot_name LIKE 'AL%' OR
      t_ph_lot_name LIKE 'EL%' OR
      t_ph_lot_name LIKE 'FN%' OR
      t_ph_lot_name LIKE 'FE%' OR
      t_ph_lot_name LIKE 'FA%' OR
      t_ph_lot_name LIKE 'LS%' OR
      t_ph_lot_name LIKE 'LR%' OR
      t_ph_lot_name LIKE 'CF%' OR
      t_ph_lot_name LIKE 'CG%' OR
      t_ph_lot_name LIKE 'CE%' OR
      t_ph_lot_name LIKE 'DK%' OR
      t_ph_lot_name LIKE 'BF%' OR
      t_ph_lot_name LIKE 'BB%' OR
      t_ph_lot_name LIKE 'BK%' OR
      t_ph_lot_name LIKE 'BH%' OR
      t_ph_lot_name LIKE 'BQ%' OR
      t_ph_lot_name LIKE 'BP%' OR
      t_ph_lot_name LIKE 'BN%' OR
      t_ph_lot_name LIKE 'CB%' OR
      t_ph_lot_name LIKE 'KE%' OR
      t_ph_lot_name LIKE 'CA%' OR
      t_ph_lot_name LIKE 'DD%' OR
      t_ph_lot_name LIKE 'DA%' OR
      t_ph_lot_name LIKE 'DN%' OR
      t_ph_lot_name LIKE 'PL%' OR
      t_ph_lot_name LIKE 'LP%' OR
      t_ph_lot_name LIKE 'RI%' OR
      t_ph_lot_name LIKE 'PY%' OR
      t_ph_lot_name LIKE 'LB%')
group by lot_prefix, src_sys
