-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### how to find out the current version of a FORM

-- COMMAND ----------

select
concat(major_version_number__v, '.', minor_version_number__v) as version
from gms_us_lake.gmsgq_veeva_documents
WHERE document_number__v like 'FORM-278447'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # get material numbers from SPEC number / or vice versa

-- COMMAND ----------

-- Veeva Metadata⁠, part_number__c contains the material number(s):
select
title__v as DocumentName
,document_number__v as DocumentNumber
,part_number__c as MaterialNumber
from gms_us_lake.gmsgq_veeva_documents
where document_number__v like 'SPEC-235806'

-- COMMAND ----------

-- Veeva Metadata⁠, example:
select
title__v as DocumentName,
type__v as DocumentType,
document_number__v as DocumentNumber,
previous_document_number__c as LegacyDocumentNumber,
latest_version__v, -- either true or false
status__v as Status, -- effective, final,...?
major_version_number__v,
minor_version_number__v
,global_version_id__sys
,*
from gms_us_lake.gmsgq_veeva_documents
WHERE type__v like 'Form'
and area_specifics__c like 'Vienna - QA%'
and title__v like 'CERTIFICATE OF ANALYSIS%'
-- WHERE document_number__v like 'FORM-278447%'

-- COMMAND ----------

describe gms_us_lake.gmsgq_veeva_documents

-- COMMAND ----------

-- Veeva Metadata⁠, example:
select
title__v as DocumentName,
type__v as DocumentType,
document_number__v as DocumentNumber,
previous_document_number__c as LegacyDocumentNumber,
latest_version__v, -- either true or false
status__v as Status, -- effective, final,...?
major_version_number__v,
minor_version_number__v,
area_specifics__c,
-- area_specifics__c.name,
-- business_unit__c.name__v,
business_unit__c,
change_controlrequest_number__c,
country__v,
-- country__v.name__v,
document_language_2__c,
document_owner__c,
-- document_owner__c.name__v,
filename__v,
impacted_facilities__v,
product__v,
-- product__v.name__v,
training_applicable__c,
training_period_days__c,
impacted_departments__v,
merge_fields__v,
next_periodic_review_date__c,
obsolescence_approved__c,
obsolescence_reason__c,
qualifiedresponsible_person_signature__c,
reference_model_category__c,
-- reference_model_category__c.name__v,
*
from gms_us_lake.gmsgq_veeva_documents
-- WHERE document_number__v like 'FORM-027844%'
-- WHERE document_number__v like 'FORM-278447%'

WHERE area_specifics__c like 'Vienna%'
-- and merge_fields__v like 'true'
and type__v like 'Form'
-- and type__v not like "Record"
-- WHERE area_specifics__c like 'Vienna - QA%'
-- WHERE document_number__v like 'SOP-%'

-- COMMAND ----------

select
title__v as DocumentName,
type__v as DocumentType,
document_number__v as DocumentNumber,
previous_document_number__c as LegacyDocumentNumber,
latest_version__v, -- either true or false
status__v as Status, -- effective, final,...?
major_version_number__v,
minor_version_number__v,
area_specifics__c,
`area_specifics__c.name`,
`business_unit__c.name__v`,
business_unit__c,
change_controlrequest_number__c,
country__v,
`country__v.name__v`,
document_language_2__c,
document_owner__c,
`document_owner__c.name__v`,
filename__v,
impacted_facilities__v,
product__v,
`product__v.name__v`,
training_applicable__c,
training_period_days__c,
impacted_departments__v,
merge_fields__v,
next_periodic_review_date__c,
obsolescence_approved__c,
obsolescence_reason__c,
qualifiedresponsible_person_signature__c,
reference_model_category__c
`reference_model_category__c.name__v`
from gms_us_lake.gmsgq_veeva_documents
-- WHERE document_number__v like 'MBR-206149%'
WHERE title__v like 'MBR UND BR PL, F8, F9A, AT3%'
-- and business_unit__c like 
-- and country__v.name__v like "Austria"

-- COMMAND ----------

describe gms_us_lake.gmsgq_veeva_documents

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # searching Vienna QA Dispo Forms

-- COMMAND ----------

-- Veeva Metadata⁠, example:
select
title__v as DocumentName,
type__v as DocumentType,
document_number__v as DocumentNumber,
previous_document_number__c as LegacyDocumentNumber,
issued_date__c,
latest_version__v, -- either true or false
status__v as Status, -- effective, final,...?
major_version_number__v,
minor_version_number__v,
area_specifics__c,
`product__v.name__v` as ProductName,
`area_specifics__c.name`,
`business_unit__c.name__v`,
business_unit__c,
change_controlrequest_number__c,
country__v,
`country__v.name__v`,
document_language_2__c,
document_owner__c,
`document_owner__c.name__v`,
filename__v,
impacted_facilities__v,
product__v,
`product__v.name__v`,
training_applicable__c,
training_period_days__c,
impacted_departments__v,
`impacted_departments__v.name__v`,
`impacted_facilities__v.name__v`,
`functionoperating_unit__c.name__v`,
`owning_department__v.name__v`,
`owning_facility__v.name__v`,
merge_fields__v,
next_periodic_review_date__c,
obsolescence_approved__c,
obsolescence_reason__c,
qualifiedresponsible_person_signature__c,
reference_model_category__c,
proposed_effective_date__c,
proposed_issued_date__c,
proposed_obsolescence_date__c,
`reference_model_category__c.name__v`,
training_applicable__c,
training_required__c,
training_period_days__c,
downloadable_document__c,
ftp_source_location__v,
*
from gms_us_lake.gmsgq_veeva_documents
-- WHERE document_number__v like 'FORM-027844%'
-- WHERE document_number__v like 'FORM-278447%'

WHERE area_specifics__c like 'Vienna%'
-- and merge_fields__v like 'true'
-- and type__v like 'Form'
-- and type__v not like "Record"
-- WHERE area_specifics__c like 'Vienna - QA%'
-- and `impacted_facilities__v.name__v` like 'AUT - Vienna%'
and type__v like 'Form'
-- and document_number__v like 'FORM-%'
-- and `impacted_departments__v.name__v` like 'QA%'
and area_specifics__c like 'Vienna - QA%'
and latest_version__v is true
-- where issued_date__c like '2024-09%'
-- sort by issued_date__c desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Searching for MBRs for Vienna using backticks to use nested/hierarchial columns (column names with dots)

-- COMMAND ----------

-- Veeva Metadata⁠, example:
select
title__v as DocumentName,
type__v as DocumentType,
document_number__v as DocumentNumber,
previous_document_number__c as LegacyDocumentNumber,
issued_date__c,
latest_version__v, -- either true or false
status__v as Status, -- effective, final,...?
major_version_number__v,
minor_version_number__v,
area_specifics__c,
`product__v.name__v` as ProductName,
-- product__v as ProductName,
-- area_specifics__c.name,
-- business_unit__c.name__v,
business_unit__c,
change_controlrequest_number__c,
country__v,
-- country__v.name__v,
document_language_2__c,
document_owner__c,
-- document_owner__c.name__v,
filename__v,
impacted_facilities__v,
product__v,
-- product__v.name__v,
training_applicable__c,
training_period_days__c,
impacted_departments__v,
merge_fields__v,
next_periodic_review_date__c,
obsolescence_approved__c,
obsolescence_reason__c,
qualifiedresponsible_person_signature__c,
reference_model_category__c,
-- reference_model_category__c.name__v,
*
from gms_us_lake.gmsgq_veeva_documents
-- WHERE document_number__v like 'FORM-027844%'
-- WHERE document_number__v like 'FORM-278447%'

WHERE area_specifics__c like 'Vienna%'
-- and merge_fields__v like 'true'
-- and type__v like 'Form'
-- and type__v not like "Record"
-- WHERE area_specifics__c like 'Vienna - QA%'
-- and `impacted_facilities__v.name__v` like 'AUT - Vienna%'
and document_number__v like 'MBR-%'
and `impacted_departments__v.name__v` like 'Manufacturing Pharma%'
-- where issued_date__c like '2024-09%'
-- sort by issued_date__c desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # how old is veeva data?

-- COMMAND ----------

-- Veeva Metadata⁠, example:
select
title__v as DocumentName,
type__v as DocumentType,
document_number__v as DocumentNumber,
previous_document_number__c as LegacyDocumentNumber,
issued_date__c,
latest_version__v, -- either true or false
status__v as Status, -- effective, final,...?
major_version_number__v,
minor_version_number__v,
area_specifics__c,
`product__v.name__v` as ProductName,
-- product__v as ProductName,
-- area_specifics__c.name,
-- business_unit__c.name__v,
business_unit__c,
change_controlrequest_number__c,
country__v,
-- country__v.name__v,
document_language_2__c,
document_owner__c,
-- document_owner__c.name__v,
filename__v,
impacted_facilities__v,
product__v,
-- product__v.name__v,
training_applicable__c,
training_period_days__c,
impacted_departments__v,
merge_fields__v,
next_periodic_review_date__c,
obsolescence_approved__c,
obsolescence_reason__c,
qualifiedresponsible_person_signature__c,
reference_model_category__c,
-- reference_model_category__c.name__v,
*
from gms_us_lake.gmsgq_veeva_documents
-- WHERE document_number__v like 'FORM-027844%'
-- WHERE document_number__v like 'FORM-278447%'

WHERE
-- area_specifics__c like 'Vienna%'

-- and merge_fields__v like 'true'
-- and type__v like 'Form'
-- and type__v not like "Record"
-- WHERE area_specifics__c like 'Vienna - QA%'
-- and `impacted_facilities__v.name__v` like 'AUT - Vienna%'
-- and document_number__v like 'MBR-%'
-- and `impacted_departments__v.name__v` like 'Manufacturing Pharma%'
-- and 
issued_date__c like '2024-09-12%'
-- sort by issued_date__c desc

-- COMMAND ----------

DESCRIBE HISTORY gms_us_lake.gmsgq_veeva_documents;


-- COMMAND ----------

SELECT * FROM (DESCRIBE HISTORY gms_us_lake.gmsgq_veeva_documents) ORDER BY version DESC LIMIT 1;
