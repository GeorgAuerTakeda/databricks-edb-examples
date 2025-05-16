-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Example usage of the relevant Labware (GLIMS) EDB Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC for "old" SQL*LIMS Batches, the search via LOT_NUMBER works, for the new Labware values, only the LOT field is mapped. As mitigation the T_PH_LOT_NAME from gms_us_lake.gmsgq_glims_lot can be used after a join.
-- MAGIC Only Active Results should be shown - expressed through the ACTIVE_FLAG.
-- MAGIC If two results for one Lot exist, only the new one will be active.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC gms_us_mart.txn_lot_result_approved_lims_glbl  .... composite key: site_nm,src_system,lot_number,result_id,result_version,entry_code
-- MAGIC  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC there is an append logic - so if there is a new record with the same primary key, the previous record is marked as unactive and a new record is created with active flag = Y. You should look at active_flag = 'Y' records only. 
-- MAGIC  

-- COMMAND ----------

describe gms_us_mart.txn_lot_result_approved_lims_glbl

-- COMMAND ----------


with GLIMS as(

select
-- lot_result.LOT_NUMBER,
LOT
, coalesce(mitigation_lotname.T_PH_LOT_NAME, lot_result.LOT_NUMBER) as BATCH_ID -- merge LOT_ERP and LOTNUMBER_VALUE from both systems
, TEXT_VALUE
, NUMBER_VALUE
, operation
, component
, analysis
, method_datagroup
, measure
, testnamedetail
, testnamegeneral
, productfamily
, result_condition
, lot_result.ACTIVE_FLAG as active_flag
from gms_us_mart.txn_lot_result_approved_lims_glbl lot_result
left join gms_us_lake.gmsgq_glims_lot mitigation_lotname
ON lot_result.LOT = mitigation_lotname.LOT_NAME
)

select
*
from GLIMS

where BATCH_ID like 'A1B552%' or BATCH_ID like 'A1B519%'
-- where GLIMS.LOT_NUMBER like "B5B004/AA%"
-- and (operation like 'VN1306027 ALPHA1 ELASTE INH.ACT CHROMA' and text_value like 'completed' and result_condition like 'APPROVED' and ACTIVE_FLAG like 'Y')
-- and (operation like 'VN1306027 ALPHA1 ELASTE INH.ACT CHROMA' and text_value like 'completed')
-- or GLIMS.LOT_NUMBER like "A4A258%"
-- or GLIMS.LOT_NUMBER like "M5%"
-- or GLIMS.LOT_NUMBER like "C4%"
-- or GLIMS.LOT_NUMBER like "X1%"


-- COMMAND ----------

-- checks if BRR is available in Labware

with GLIMS as(
select
LOT
, coalesce(mitigation_lotname.T_PH_LOT_NAME, lot_result.LOT_NUMBER) as BATCH_ID -- merge LOT_ERP and LOTNUMBER_VALUE from both systems
, TEXT_VALUE
, NUMBER_VALUE
, operation
, component
, method_datagroup
, measure
, testnamedetail
, result_condition
, lot_result.ACTIVE_FLAG as active_flag
, lot_result.src_system
, SITE_NM
from gms_us_mart.txn_lot_result_approved_lims_glbl lot_result
left join gms_us_lake.gmsgq_glims_lot mitigation_lotname
ON lot_result.LOT = mitigation_lotname.LOT_NAME
)

select
*
from GLIMS
where BATCH_ID like 'A1B552%' or BATCH_ID like 'A1B519%'

-- COMMAND ----------

describe gms_us_mart.txn_lot_result_approved_lims_glbl

-- COMMAND ----------

select template, * from gms_us_lake.gmsgq_glims_lot
where template like '%RM%' -- sollte alle IM sein inkl sek. packmittel
-- plan fam JDE code: SPM
-- FP_LH -- ll 

-- COMMAND ----------

select distinct template from gms_us_lake.gmsgq_glims_lot

-- COMMAND ----------


with GLIMS as(

select
-- lot_result.LOT_NUMBER,
LOT
, coalesce(mitigation_lotname.T_PH_LOT_NAME, lot_result.LOT_NUMBER) as LOT_NUMBER -- merge LOT_ERP and LOTNUMBER_VALUE from both systems
, TEXT_VALUE
, NUMBER_VALUE
, lot_result.MEASURE
, UNITS
, ANALYSIS
, TESTNAMEDETAIL
, STATUS -- should always be A since only approved values will be here
, ENTRY_DATE
, TESTNAMEGENERAL
, STAGE_NAME
, lot_result.ACTIVE_FLAG
, SRC_SYSTEM
, RESULT_ID
, RESULT_VERSION
, ENTRY_CODE
, MATERIAL_TYPE
, MATERIAL_CODE
, MATERIAL_NAME
, SAMPLE_ID
, USER_SAMPLEID
, STAGE_NAME
, WORKLIST_ID
, TASK_ID
-- ,*
from gms_us_mart.txn_lot_result_approved_lims_glbl lot_result
left join gms_us_lake.gmsgq_glims_lot mitigation_lotname
ON lot_result.LOT = mitigation_lotname.LOT_NAME
-- where SITE_NM like "Vienna%"
-- where LOT_NUMBER like "VIPLA0001A%"
-- where mitigation_lotname.T_PH_LOT_NAME like "VIPLB0390A" -- new Labware Results can be found with this column until the LOT_NUMBER is mapped.
-- where coalesce(mitigation_lotname.T_PH_LOT_NAME, LOT) like "VIPLA%" -- new Labware Results can be found with this column until the LOT_NUMBER is mapped.
)

select
*
from GLIMS
-- where LOT_NUMBER like "ELB0111%"
-- where GLIMS.LOT_NUMBER like "Y2T003AA%"
where GLIMS.LOT_NUMBER like "Y2B%"
-- where GLIMS.LOT_NUMBER like "A4B190%"
-- where LOT_NUMBER like "VIPLB0033A%"
-- and SRC_SYSTEM like "%LIMS"
-- where (LOT_NUMBER like "VIPL%" AND COMPONENT like "Parvo B19%") -- Labware
-- OR (LOT like "VIPL%" AND TESTNAMEDETAIL like "Parvo B19%") --SQL*LIMS


-- and lot_result.ACTIVE_FLAG like "Y"
-- and TESTNAMEDETAIL like "Fibrinogen"
-- and TESTNAMEDETAIL not like ""
-- and TESTNAMEDETAIL like "Sterility%"
-- sort by LOT_NUMBER ASC

-- COMMAND ----------

select * from gms_us_mart.txn_lot_result_approved_lims_glbl

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # how old is the data?
-- MAGIC

-- COMMAND ----------

describe history gms_us_mart.txn_lot_result_approved_lims_glbl

-- COMMAND ----------

-- explain select * from gms_us_mart.txn_lot_result_approved_lims_glbl
describe table gms_us_mart.txn_lot_result_approved_lims_glbl

-- COMMAND ----------

describe detail gms_us_mart.txn_lot_result_approved_lims_glbl

-- COMMAND ----------

SHOW COLUMNS IN gms_us_mart.txn_lot_result_approved_lims_glbl


-- COMMAND ----------

-- MAGIC %md
-- MAGIC # table tests

-- COMMAND ----------

select
*
from gms_us_mart.txn_lot_result_approved_lims_glbl
where (LOT_NUMBER like "VIPLA0100%" AND COMPONENT like "Parvo B19%") -- Labware
OR (LOT like "VIPLB0100%" AND TESTNAMEDETAIL like "Parvo B19%") --SQL*LIMS



-- COMMAND ----------

select * from gms_us_mart.txn_lot_result_approved_lims_glbl
where SITE_NM like "Vienna"
-- where LOT_NUMBER like "ELB%"

-- COMMAND ----------

select * from gms_us_lake.gmsgq_glims_lot
-- sort by 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC gms_us_mart.txn_test_approved_lims_glbl ... .TEST_KEY
-- MAGIC  

-- COMMAND ----------

select * from GMS_US_mart.txn_test_approved_lims_glbl

-- COMMAND ----------

-- MAGIC %md
-- MAGIC gms_us_mart.txn_result_approved_lims_glbl  ... RESULT_KEY
-- MAGIC  

-- COMMAND ----------

select * from gms_us_mart.txn_result_approved_lims_glbl
 

-- COMMAND ----------

select
lot_result.LOT_NUMBER,
LOT,
mitigation_lotname.T_PH_LOT_NAME as LOT_ERP, -- column as mitigattion, as long as there is no data in LOT_NUMBER
NUMBER_VALUE,
UNITS,
ANALYSIS,
TESTNAMEDETAIL,
component, -- is the test name from SQL*LIMS
STATUS, -- should always be A since only approved values will be here
ENTRY_DATE,
TESTNAMEGENERAL,
lot_result.ACTIVE_FLAG,
SRC_SYSTEM,
RESULT_ID,
RESULT_VERSION,
ENTRY_CODE
from gms_us_mart.txn_lot_result_approved_lims_glbl lot_result
left join gms_us_lake.gmsgq_glims_lot mitigation_lotname
ON lot_result.LOT = mitigation_lotname.LOT_NAME
-- where SITE_NM like "Vienna%"
where lot_result.LOT_NUMBER like "VIPLA0001A%"
-- where mitigation_lotname.T_PH_LOT_NAME like "VIPLB0190A" -- new Labware Results can be found with this column until the LOT_NUMBER is mapped.
and lot_result.ACTIVE_FLAG like "Y"
-- and TESTNAMEDETAIL like "Fibrinogen"
-- and TESTNAMEDETAIL not like ""
-- and TESTNAMEDETAIL like "Sterility%"
-- sort by LOT_NUMBER ASC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # EDB LIP transfer project

-- COMMAND ----------

select * from gms_us_lake.gmsgq_glims_lot
where t_ph_lot_name like 'VIPLC0001%'
and closed = 'T'
and signed = 'T'
and active_flage = 'T'
