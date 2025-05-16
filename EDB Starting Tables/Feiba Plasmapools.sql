-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Explanation
-- MAGIC
-- MAGIC Table gms_us_mart.ref_gmsgq_plasma_lot_genealogy_glbl can be used to find out which volumina went into which final container lots.
-- MAGIC
-- MAGIC Example Feiba:
-- MAGIC Eluat FA...
-- MAGIC Bulk: FN...
-- MAGIC Final Container: F2...
-- MAGIC FIN: F2...AA
-- MAGIC
-- MAGIC VN-CECS0007-01-FDS_Functional Design Specification for Lot reports & Plasma Reports (v7.0)
-- MAGIC https://takeda-quality.veevavault.com/ui/#doc_info/793517/7/0=&idx=9&pt=rl&sm=&tvsl=Jml2cD0xJml2dj1DT01QQUNUJnZpZXdJZD1yZWNlbnQmdGFiSWQ9MFRCMDAwMDAwMDEzMDA4JmZhY2V0c1VuY2hhbmdlZD1mYWxzZSZpdnM9RG9jTGFzdFZpZXdlZCZpdm89ZGVzYyZpbnRlcm5hbEZpbHRlcnMlNUIlNUQ9X2RvY1ZlcnNpb25GaWx0ZXIlM0ElNUIlMjJ0cnVlJTIyJTVE
-- MAGIC
-- MAGIC Lotnummernsystem Wien (v5.0)
-- MAGIC https://takeda-quality.veevavault.com/ui/#doc_info/640556/5/0
-- MAGIC
-- MAGIC Lotnummernsystem für Plasmapools (v1.0)
-- MAGIC https://takeda-quality.veevavault.com/ui/#doc_info/591081/1/0=&idx=0&pt=rl&sm=&tvsl=Jml2cD0xJml2dj1DT01QQUNUJnZpZXdJZD1yZWNlbnQmdGFiSWQ9MFRCMDAwMDAwMDEzMDA4JmZhY2V0c1VuY2hhbmdlZD1mYWxzZSZpdnM9RG9jTGFzdFZpZXdlZCZpdm89ZGVzYyZpbnRlcm5hbEZpbHRlcnMlNUIlNUQ9X2RvY1ZlcnNpb25GaWx0ZXIlM0ElNUIlMjJ0cnVlJTIyJTVE
-- MAGIC
-- MAGIC JDE Global ERP System Manual (QM-000414) (v6.0)
-- MAGIC https://takeda-quality.veevavault.com/ui/#doc_info/417210/6/0?idx=6&pt=rl&sm=&tvsl=Jml2cD0xJml2dj1DT01QQUNUJnZpZXdJZD1yZWNlbnQmdGFiSWQ9MFRCMDAwMDAwMDEzMDA4JmZhY2V0c1VuY2hhbmdlZD1mYWxzZSZpdnM9RG9jTGFzdFZpZXdlZCZpdm89ZGVzYyZpbnRlcm5hbEZpbHRlcnMlNUIlNUQ9X2RvY1ZlcnNpb25GaWx0ZXIlM0ElNUIlMjJ0cnVlJTIyJTVE&anQS=page19&annotate=false

-- COMMAND ----------

select distinct
root_lot_num
, lot_num -- aka batch_id in JDE tables
, child_lot_num
, prod_qty
, plasma_vol
, child_lot_num_prev_hier
, hier_lvl
from gms_us_mart.ref_gmsgq_plasma_lot_genealogy_glbl
-- where root_lot_num like 'FAA0183%'
where root_lot_num like 'F2A046%'
                       
-- where root_lot_num like 'F2B%'

-- COMMAND ----------

select distinct
root_lot_num
, lot_num -- aka batch_id in JDE tables
, child_lot_num
, prod_qty
, plasma_vol
, child_lot_num_prev_hier
, hier_lvl
from gms_us_mart.ref_gmsgq_plasma_lot_genealogy_glbl
-- where root_lot_num like 'FAA0183%'
where root_lot_num like 'ELC0171%'
