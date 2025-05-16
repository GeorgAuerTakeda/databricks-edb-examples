-- Databricks notebook source
-- MAGIC %python
-- MAGIC table_list = ['gmsgq_werum_pasx_vienna_actualelementmarker', 'gmsgq_werum_pasx_vienna_attributivespecprop', 'gmsgq_werum_pasx_vienna_attributivespecpropactualvalue', 'gmsgq_werum_pasx_vienna_basicoperation', 'gmsgq_werum_pasx_vienna_bundlecreationbf', 'gmsgq_werum_pasx_vienna_bundlecreationbfactualvalue', 'gmsgq_werum_pasx_vienna_commonbf', 'gmsgq_werum_pasx_vienna_datespecprop', 'gmsgq_werum_pasx_vienna_datespecpropactualvalue', 'gmsgq_werum_pasx_vienna_dcsbfactualvalue', 'gmsgq_werum_pasx_vienna_dcsbfbatchstateactualvalue', 'gmsgq_werum_pasx_vienna_dcsbfinvalueactualvalue', 'gmsgq_werum_pasx_vienna_dcsbfoutvalueactualvalue', 'gmsgq_werum_pasx_vienna_dcsbfsignalreceiveractualvalue', 'gmsgq_werum_pasx_vienna_dcsbfsignalsenderactualvalue', 'gmsgq_werum_pasx_vienna_dcsformulasignalsender', 'gmsgq_werum_pasx_vienna_dcsinvalue', 'gmsgq_werum_pasx_vienna_dcsmbrelement', 'gmsgq_werum_pasx_vienna_dcsoutvalue', 'gmsgq_werum_pasx_vienna_dcsrecipebf', 'gmsgq_werum_pasx_vienna_dcsscadabf', 'gmsgq_werum_pasx_vienna_dcsstatetransition', 'gmsgq_werum_pasx_vienna_decisionactualvalue', 'gmsgq_werum_pasx_vienna_deviationevent', 'gmsgq_werum_pasx_vienna_deviationeventchangeitem', 'gmsgq_werum_pasx_vienna_deviationeventpriority', 'gmsgq_werum_pasx_vienna_deviationeventtypepriority', 'gmsgq_werum_pasx_vienna_documentlistentry', 'gmsgq_werum_pasx_vienna_documentspecprop', 'gmsgq_werum_pasx_vienna_documentspecpropactualvalue', 'gmsgq_werum_pasx_vienna_ebractualelementmarker', 'gmsgq_werum_pasx_vienna_ebrattributivespecprop', 'gmsgq_werum_pasx_vienna_ebrbasicfunctiontimeslot', 'gmsgq_werum_pasx_vienna_ebrbasicoperation', 'gmsgq_werum_pasx_vienna_ebrbundlecreationbf', 'gmsgq_werum_pasx_vienna_ebrcommonbf', 'gmsgq_werum_pasx_vienna_ebrcondition', 'gmsgq_werum_pasx_vienna_ebrdatespecprop', 'gmsgq_werum_pasx_vienna_ebrdcsbfbatchstate', 'gmsgq_werum_pasx_vienna_ebrdcsbfformulasignalsender', 'gmsgq_werum_pasx_vienna_ebrdcsbfinvalue', 'gmsgq_werum_pasx_vienna_ebrdcsbfoutvalue', 'gmsgq_werum_pasx_vienna_ebrdcsinvaluesignalreceiver', 'gmsgq_werum_pasx_vienna_ebrdcsrecipebf', 'gmsgq_werum_pasx_vienna_ebrdcsscadabf', 'gmsgq_werum_pasx_vienna_ebrdcsstatetransitionsignalrec', 'gmsgq_werum_pasx_vienna_ebrdcsstatetransitionsignalsen', 'gmsgq_werum_pasx_vienna_ebrdcsstatevaluesignalsender', 'gmsgq_werum_pasx_vienna_ebrdcsstatussignalreceiver', 'gmsgq_werum_pasx_vienna_ebrdecision', 'gmsgq_werum_pasx_vienna_ebrdocumentlistentry', 'gmsgq_werum_pasx_vienna_ebrdocumentspecprop', 'gmsgq_werum_pasx_vienna_ebrelementcontextinformation', 'gmsgq_werum_pasx_vienna_ebreqmallocationbf', 'gmsgq_werum_pasx_vienna_ebreqmassemblybf', 'gmsgq_werum_pasx_vienna_ebreqmassemblybfpos', 'gmsgq_werum_pasx_vienna_ebreqmdeallocationbf', 'gmsgq_werum_pasx_vienna_ebreqmdisassemblybf', 'gmsgq_werum_pasx_vienna_ebreqmidentificationbf', 'gmsgq_werum_pasx_vienna_ebreqmstatuschangebf', 'gmsgq_werum_pasx_vienna_ebrequipmentlistpos', 'gmsgq_werum_pasx_vienna_ebrexpirationdatespecprop', 'gmsgq_werum_pasx_vienna_ebrfieldvalue', 'gmsgq_werum_pasx_vienna_ebrformulaspecprop', 'gmsgq_werum_pasx_vienna_ebrgenericlabelprintbf', 'gmsgq_werum_pasx_vienna_ebridentitycheckbf', 'gmsgq_werum_pasx_vienna_ebrlistspecprop', 'gmsgq_werum_pasx_vienna_ebrlistspecpropelement', 'gmsgq_werum_pasx_vienna_ebrmanufacturingdatespecprop', 'gmsgq_werum_pasx_vienna_ebrmaterialinput', 'gmsgq_werum_pasx_vienna_ebrmaterialoutput', 'gmsgq_werum_pasx_vienna_ebrmeasuredvaluespecprop', 'gmsgq_werum_pasx_vienna_ebrmsiactualvalueparameter', 'gmsgq_werum_pasx_vienna_ebrmsiinterfacebf', 'gmsgq_werum_pasx_vienna_ebrmsiqualifier', 'gmsgq_werum_pasx_vienna_ebrmsisetvalueparameter', 'gmsgq_werum_pasx_vienna_ebroperationaltimespecprop', 'gmsgq_werum_pasx_vienna_ebrsetcxbf', 'gmsgq_werum_pasx_vienna_ebrsignalreceivebf', 'gmsgq_werum_pasx_vienna_ebrsignalsendbf', 'gmsgq_werum_pasx_vienna_ebrsopspecprop', 'gmsgq_werum_pasx_vienna_ebrspecificationbf', 'gmsgq_werum_pasx_vienna_ebrstartsfobf', 'gmsgq_werum_pasx_vienna_ebrstockcreationbf', 'gmsgq_werum_pasx_vienna_ebrtakeoutbf', 'gmsgq_werum_pasx_vienna_ebrtextspecprop', 'gmsgq_werum_pasx_vienna_ebrtimerbf', 'gmsgq_werum_pasx_vienna_ebryielddeterminationbf', 'gmsgq_werum_pasx_vienna_edi_domainvaluetranslations', 'gmsgq_werum_pasx_vienna_edocumentcontent', 'gmsgq_werum_pasx_vienna_edocumentinformation', 'gmsgq_werum_pasx_vienna_electronicbatchrecord', 'gmsgq_werum_pasx_vienna_elementmarker', 'gmsgq_werum_pasx_vienna_eqm_equipment', 'gmsgq_werum_pasx_vienna_eqm_equipmenttype', 'gmsgq_werum_pasx_vienna_eqm_equipmenttypeversion', 'gmsgq_werum_pasx_vienna_eqm_equipmentversion', 'gmsgq_werum_pasx_vienna_eqmallocationbf', 'gmsgq_werum_pasx_vienna_eqmallocationbfactualvalue', 'gmsgq_werum_pasx_vienna_eqmassemblybf', 'gmsgq_werum_pasx_vienna_eqmassemblybfactualvalue', 'gmsgq_werum_pasx_vienna_eqmassemblybfposactualvalue', 'gmsgq_werum_pasx_vienna_eqmassemblybfposexecutiondatai', 'gmsgq_werum_pasx_vienna_eqmdeallocationbf', 'gmsgq_werum_pasx_vienna_eqmdeallocationbfactualvalue', 'gmsgq_werum_pasx_vienna_eqmdisassemblybf', 'gmsgq_werum_pasx_vienna_eqmdisassemblybfactualvalue', 'gmsgq_werum_pasx_vienna_eqmidentificationbf', 'gmsgq_werum_pasx_vienna_eqmidentificationbfactualvalue', 'gmsgq_werum_pasx_vienna_eqmstatuschangebf', 'gmsgq_werum_pasx_vienna_eqmstatuschangebfactualvalue', 'gmsgq_werum_pasx_vienna_equipmentlistpos', 'gmsgq_werum_pasx_vienna_expirationdatespecprop', 'gmsgq_werum_pasx_vienna_expirationdatespecpropactualva', 'gmsgq_werum_pasx_vienna_fieldvalueactualvalue', 'gmsgq_werum_pasx_vienna_formulaspecprop', 'gmsgq_werum_pasx_vienna_formulaspecpropactualvalue', 'gmsgq_werum_pasx_vienna_genericlabelprintbf', 'gmsgq_werum_pasx_vienna_genericlabelprintbfactualvalue', 'gmsgq_werum_pasx_vienna_handlingunit', 'gmsgq_werum_pasx_vienna_identitycheckbf', 'gmsgq_werum_pasx_vienna_identitycheckbfactualvalue', 'gmsgq_werum_pasx_vienna_listspecprop', 'gmsgq_werum_pasx_vienna_listspecpropactualvalue', 'gmsgq_werum_pasx_vienna_listspecpropelement', 'gmsgq_werum_pasx_vienna_manufacturingdatespecprop', 'gmsgq_werum_pasx_vienna_manufacturingdatespecpropactua', 'gmsgq_werum_pasx_vienna_manufacturingorder', 'gmsgq_werum_pasx_vienna_manufacturingorderstatuschange', 'gmsgq_werum_pasx_vienna_masterbatchrecord', 'gmsgq_werum_pasx_vienna_materialinput', 'gmsgq_werum_pasx_vienna_materialinputexecutiondataitem', 'gmsgq_werum_pasx_vienna_materialoutput', 'gmsgq_werum_pasx_vienna_md_unitofmeasurement', 'gmsgq_werum_pasx_vienna_measuredvaluespecprop', 'gmsgq_werum_pasx_vienna_mmd_material', 'gmsgq_werum_pasx_vienna_mmd_materialusetype', 'gmsgq_werum_pasx_vienna_mmd_materialversion', 'gmsgq_werum_pasx_vienna_mvspbooleanactualvalue', 'gmsgq_werum_pasx_vienna_mvspmultivalueactualvalue', 'gmsgq_werum_pasx_vienna_mvspsinglevalueactualvalue', 'gmsgq_werum_pasx_vienna_mvspvalueactualvalue', 'gmsgq_werum_pasx_vienna_operationaltimespecprop', 'gmsgq_werum_pasx_vienna_optimespecpropactualvalue', 'gmsgq_werum_pasx_vienna_orc_manufacturingorderhead', 'gmsgq_werum_pasx_vienna_orc_takeoutlogentry', 'gmsgq_werum_pasx_vienna_pu_productionunit', 'gmsgq_werum_pasx_vienna_pu_productionunitgroup', 'gmsgq_werum_pasx_vienna_setcxbf', 'gmsgq_werum_pasx_vienna_setcxbfactualvalue', 'gmsgq_werum_pasx_vienna_shopfloororder', 'gmsgq_werum_pasx_vienna_shopfloororderstatuschangesign', 'gmsgq_werum_pasx_vienna_signalreceivebf', 'gmsgq_werum_pasx_vienna_signalreceivebfactualvalue', 'gmsgq_werum_pasx_vienna_signalsendbf', 'gmsgq_werum_pasx_vienna_signalsendbfactualvalue', 'gmsgq_werum_pasx_vienna_signature', 'gmsgq_werum_pasx_vienna_singlecheckresult', 'gmsgq_werum_pasx_vienna_sopspecprop', 'gmsgq_werum_pasx_vienna_sopspecpropactualvalue', 'gmsgq_werum_pasx_vienna_speccondition', 'gmsgq_werum_pasx_vienna_specdecision', 'gmsgq_werum_pasx_vienna_specificationbf', 'gmsgq_werum_pasx_vienna_specificationbfactualvalue', 'gmsgq_werum_pasx_vienna_specificationintegritycheckres', 'gmsgq_werum_pasx_vienna_startsfobf', 'gmsgq_werum_pasx_vienna_startsfobfactualvalue', 'gmsgq_werum_pasx_vienna_stock_batch', 'gmsgq_werum_pasx_vienna_stockcreationbf', 'gmsgq_werum_pasx_vienna_stockcreationbfactualvalue', 'gmsgq_werum_pasx_vienna_takeoutbf', 'gmsgq_werum_pasx_vienna_takeoutbfactualvalue', 'gmsgq_werum_pasx_vienna_textspecprop', 'gmsgq_werum_pasx_vienna_textspecpropactualvalue', 'gmsgq_werum_pasx_vienna_timerbf', 'gmsgq_werum_pasx_vienna_timerbfactualvalue', 'gmsgq_werum_pasx_vienna_users', 'gmsgq_werum_pasx_vienna_yielddeterminationbf', 'gmsgq_werum_pasx_vienna_yielddeterminationbfactualvalu']
-- MAGIC

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC # vienna pasx

-- COMMAND ----------

select * from gms_us_lake.gmsgq_werum_pasx_vienna_orc_manufacturingorderhead

-- COMMAND ----------

select ORDER_NUM, BATCH_ID, MAT_DESC, batch_stat_cd from gms_us_hub.ref_materialbatch_erp_glbl
where order_num IS NOT NULL
and ORDER_CATG_CD = 'WO'

-- COMMAND ----------

with pasx as (
select signatureid, 
       signatureaction
       ,username
      --  ,regexp_extract(relatedobject, '<td>(.*?)</td>', 1) as wo_num
       ,REPLACE(REGEXP_EXTRACT(relatedobject, '<td>(.*?)</td>', 1), 'WO', '') as wo_num

       ,signdate

       ,pasx.AUD_LD_DTS
       ,pasx.AUD_UPD_DTS
       ,pasx.AUD_CRT_DTS
      --  ,*
from gms_us_lake.gmsgq_werum_pasx_vienna_signature pasx
where pasx.ACTIVE_FLAG = 'Y'
and signatureaction = 'Freigabe von BRRWOLIMS Batch Record Report - 00000003 - QualityApproval'
and signatureid = 'ApproveBatchRecordReport'
),
mb as (
select distinct
ORDER_NUM
, BATCH_ID
, MAT_DESC
, batch_stat_cd 
from gms_us_hub.ref_materialbatch_erp_glbl
where order_num IS NOT NULL
and ORDER_CATG_CD = 'WO'
-- and so.PLANT_ID like '%VN%'
),
worker as (
      select worker, ntwrk_id from usprd_corp.corp_us_mart.ref_worker_data_edb_glbl
)
select BATCH_ID, worker, signdate from pasx
-- select BATCH_ID, batch_stat_cd, worker, signdate from pasx
inner join mb
on pasx.wo_num = mb.ORDER_NUM
inner join worker
on pasx.username = worker.ntwrk_id
-- where batch_stat_cd != ' '
order by BATCH_ID asc

-- select * from mb
-- where BATCH_ID like '%FAWL1C047%'
-- where REL_ORD_NUM like '%6026390%'

