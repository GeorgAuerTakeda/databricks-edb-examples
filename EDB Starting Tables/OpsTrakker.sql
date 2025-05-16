-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Examples – List plants by siteID

-- COMMAND ----------

select siteID, plantID, plantdescription 
from gms_us_lake.gmsgq_opstrakker_commonplant
order by siteid,plantid

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # List active Forms from plant Vienna

-- COMMAND ----------

select 
  formid,max(formversion),plantid,formdescription
from
   gms_us_lake.gmsgq_opstrakker_form 
where plantid like "AT%"
group by formid, plantid,formdescription

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # List active Forms from plant UAT

-- COMMAND ----------

select 
  formid,max(formversion),plantid,formdescription
from
   gms_us_lake.gmsgq_opstrakker_form 
where plantid="UAT" and formstatus=2 
group by formid, plantid,formdescription

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # List workflow (eRecords) of a form

-- COMMAND ----------

select max(AUD_CRT_DTS) from gms_us_lake.gmsgq_opstrakker_workflow

-- COMMAND ----------

select workflowid,workflowstatus,formid,formversion, plantid, *
from gms_us_lake.gmsgq_opstrakker_workflow
where plantid like 'VN%'
and formid = '0000286_SOP-244552'
and formversion = '1.011'

-- where plantid="UAT"
-- and formid="Paracetamol_AS01"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # List Parameters of a eRecord/workflow

-- COMMAND ----------

select 
  workflowid, itemseqnumber, parameterlabel, repnumber, parametervalue
from 
  gms_us_lake.gmsgq_opstrakker_workflowitemparameter 
where  
  workflowid='100724-150607077' and
  parametervalue is not null
order by itemseqnumber,parameterseqnumber

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # List all worflow data from an formid

-- COMMAND ----------

select 
  wf.workflowid, itemseqnumber, parameterlabel, repnumber, parametervalue
from 
  gms_us_lake.gmsgq_opstrakker_workflow wf
left join
  gms_us_lake.gmsgq_opstrakker_workflowitemparameter wf_parameter
on
  wf.workflowid = wf_parameter.workflowid 
where  
  formid = '000286_SOP-244552'
--   workflowid='031325-152132562' and
and
  parametervalue is not null
order by itemseqnumber,parameterseqnumber

-- COMMAND ----------

select 
--   wf.workflowid, itemseqnumber, parameterlabel, repnumber, parametervalue
  wf.workflowstatus, wf.modifieddate, wf.oncompletionexpression, wf_parameter.*
from 
  gms_us_lake.gmsgq_opstrakker_workflow wf
left join
  gms_us_lake.gmsgq_opstrakker_workflowitemparameter wf_parameter
on
  wf.workflowid = wf_parameter.workflowid 
where  
  wf.ACTIVE_FLAG = 'Y'
and
  formid = '000286_SOP-244552'
and
  wf_parameter.workflowid is not null
and
  plantid = 'AT-VIE'
and 
  wf.firstreviewdate is not null
-- and 
--   wf.secondreviewdate is not null
-- and 
--   oncompletionexpression is not null
order by itemseqnumber,parameterseqnumber
