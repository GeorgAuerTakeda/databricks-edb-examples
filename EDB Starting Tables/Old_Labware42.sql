-- Databricks notebook source
select * from GMS_US_mart.txn_test_approved_lims_glbl

-- COMMAND ----------

select * from gms_us_mart.txn_result_approved_lims_glbl

-- COMMAND ----------

-- MAGIC %md
-- MAGIC gms_us_mart.txn_lot_result_approved_lims_glbl  .... composite key: site_nm,src_system,lot_number,result_id,result_version,entry_code

-- COMMAND ----------

select * from gms_us_mart.txn_lot_result_approved_lims_glbl

-- COMMAND ----------

-- MAGIC %md
-- MAGIC site_nm,src_system,lot_number,result_id,result_version,entry_code 
-- MAGIC  

-- COMMAND ----------

select * from GMS_US_HUB.TXN_TEST_LIMS_GLBL
-- GMS_US_HUB.TXN_RESULT_LIMS_GLBL

-- COMMAND ----------

select * from GMS_US_HUB.TXN_RESULT_LIMS_GLBL


-- COMMAND ----------

select * from GMS_US_HUB.txn_result_spec_lims_glbl

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## labware search

-- COMMAND ----------

-- select * FROM gms_us_hub.ref_samples_lims_glbl
select 
  T_PH_LOT_NAME,
  *
 FROM gms_us_hub.ref_lot_lims_glbl
left join gms_us_hub.ref_lot_sampling_point_lims_glbl
on ref_lot_lims_glbl.LOT_ID = ref_lot_sampling_point_lims_glbl.LOT_NAME
-- left join gms_us_hub.ref_product_spec_lims_glbl
-- on ref_lot_lims_glbl.entry_code = ref_lot_sampling_point_lims_glbl.entry_code
-- left join gms_us_hub.ref_qa_stage_lims_glbl
-- on ref_lot_lims_glbl.LOT_ID = ref_qa_stage_lims_glbl.LOT_ID
left join gms_us_hub.txn_result_lims_glbl
on ref_lot_lims_glbl.LOT_ID = txn_result_lims_glbl.LOT_FK

-- where SITE_NM like 'Vienna'
where ref_lot_lims_glbl.T_PH_LOT_NAME like 'EL%'

-- COMMAND ----------

-- select * from gms_us_hub.txn_result_lims_glbl
select nm, num_val from gms_us_hub.txn_test_lims_glbl
-- where hidden like False

-- COMMAND ----------

select * from gms_us_hub.ref_product_spec_lims_glbl
-- where site_nm like "V%"

-- COMMAND ----------

select * from gms_us_hub.ref_lot_sampling_point_lims_glbl
-- where SITE_NM like "V%"

-- COMMAND ----------

WITH 
lot as (
  SELECT
  *
  FROM gms_us_lake.gmsgq_glims_lot --JDE: F4108 "Lot Master"
),
results as (
  SELECT
  *
  -- LOT_NUMBER,
  -- NAME as Test_Name,
  -- AVERAGE_VALUE
  FROM gms_us_lake.gmsgq_glims_lot_result --Labware
)
select
  T_PH_LOT_NAME as BATCH_ID,
  NAME as Test_Name,
  AVERAGE_VALUE,
  Units,
  ANALYSIS,
  STAGE,
  SPEC_TYPE,
  results.IN_SPEC
  ,*

from
lot
JOIN results on lot.LOT_NUMBER = results.LOT_NUMBER
-- where T_PH_LOT_NAME like 'ELB0101' 
where T_PH_LOT_NAME like 'A4%' 
-- where T_PH_LOT_NAME like 'A4B087%'
-- where T_PH_LOT_NAME like 'A4B111%'
-- where T_PH_LOT_NAME like 'VIPL%'
-- where T_PH_LOT_NAME like 'A4B111%'
-- where STAGE like 'HEAT_TREATM_INCUB_CHN'
-- where SPEC_TYPE like 'VN_VIS_INSP'
-- where results.PRODUCT like 'VN_HA_FUV_20PER_CHN'
-- and NAME like "%Date"
-- where T_PH_LOT_NAME like 'A4B%'
-- where T_PH_LOT_NAME like 'AF%'
-- where T_PH_LOT_NAME like 'AN%'
-- where T_PH_LOT_NAME like 'AL%'
-- where T_PH_LOT_NAME like 'A4B147%'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## labware extra tables

-- COMMAND ----------

select * from gms_us_lake.gmsgq_glims_analysis

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## labware lake - demo join

-- COMMAND ----------

WITH 
lot AS (
  SELECT
    *
  FROM gms_us_lake.gmsgq_glims_lot --JDE: F4108 "Lot Master"
),
results AS (
  SELECT
    *
  FROM gms_us_lake.gmsgq_glims_lot_result --Labware
),
merged_table AS (
  SELECT
    T_PH_LOT_NAME as BATCH_ID,
    NAME as Test_Name,
    AVERAGE_VALUE,
    Units,
    ANALYSIS,
    STAGE,
    SPEC_TYPE,
    results.IN_SPEC
    -- T_PH_LOT_NAME AS Lot,
    -- *
  FROM
    lot
    JOIN results ON lot.LOT_NUMBER = results.LOT_NUMBER

)
SELECT
  *
FROM
  merged_table
WHERE BATCH_ID LIKE 'ANB%'
limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## labware pivoting test values

-- COMMAND ----------

WITH 
lot AS (
  SELECT
    *
  FROM gms_us_lake.gmsgq_glims_lot --JDE: F4108 "Lot Master"
),
results AS (
  SELECT
    *
  FROM gms_us_lake.gmsgq_glims_lot_result --Labware
),
merged_table AS (
  SELECT
    trim(T_PH_LOT_NAME) as BATCH_ID,
    NAME as Test_Name,
    AVERAGE_VALUE,
    Units,
    ANALYSIS,
    STAGE,
    SPEC_TYPE,
    results.IN_SPEC,
    -- T_PH_LOT_NAME AS Lot,
    AVERAGE_VALUE,
    CASE WHEN Test_Name = 'Production Date' THEN AVERAGE_VALUE ELSE NULL END AS Date_of_Manufacture,
    CASE WHEN Test_Name = 'Number of Filled Final Container' THEN AVERAGE_VALUE ELSE NULL END AS Number_of_Filled_Final_Container,
    CASE WHEN Test_Name = 'Quantity for Release' THEN AVERAGE_VALUE ELSE NULL END AS Quantity_for_Release,
    CASE WHEN Test_Name = 'Expiry Date' THEN AVERAGE_VALUE ELSE NULL END AS Expiry_Date,
    CASE WHEN Test_Name = 'Visual Control' THEN AVERAGE_VALUE ELSE NULL END AS Visual_Control,
    CASE WHEN Test_Name = 'Visual Control completed' THEN AVERAGE_VALUE ELSE NULL END AS Visual_Control_Completed,
    CASE WHEN Test_Name = 'Protein (Nitrogen corrected) [mg/ml]' THEN AVERAGE_VALUE ELSE NULL END AS Protein_Nitrogen_Corrected,
    CASE WHEN Test_Name = 'Albumin' THEN AVERAGE_VALUE ELSE NULL END AS Protein_Composition,
    CASE WHEN Test_Name = 'Polymers + Aggregates' THEN AVERAGE_VALUE ELSE NULL END AS Polymers_Aggregates,
    CASE WHEN Test_Name = 'Polymers + Aggregates (Protein%)' THEN AVERAGE_VALUE ELSE NULL END AS Polymers_Aggregates_Protein,
    CASE WHEN Test_Name = 'Prekallikrein Activator Activ.' THEN AVERAGE_VALUE ELSE NULL END AS Prekallikrein_Activator_Activ,
    CASE WHEN Test_Name = 'Sodium [µmol/mL]' THEN AVERAGE_VALUE ELSE NULL END AS Sodium_Content,
    CASE WHEN Test_Name = 'Potassium [µmol/ml]' THEN AVERAGE_VALUE ELSE NULL END AS Potassium_Content,
    CASE WHEN Test_Name = 'Aluminium' THEN AVERAGE_VALUE ELSE NULL END AS Aluminium_Content,
    CASE WHEN Test_Name = 'Haem' THEN AVERAGE_VALUE ELSE NULL END AS Haem_Content,
    CASE WHEN Test_Name = 'pH Value' THEN AVERAGE_VALUE ELSE NULL END AS pH_Value,
    CASE WHEN Test_Name = 'Heat Stability' THEN AVERAGE_VALUE ELSE NULL END AS Heat_Stability,
    CASE WHEN Test_Name = 'N-Acetyltryptophane' THEN AVERAGE_VALUE ELSE NULL END AS N_Acetyltryptophane_Content,
    CASE WHEN Test_Name = 'Caprylic Acid [µmol/ml]' THEN AVERAGE_VALUE ELSE NULL END AS Caprylic_Acid,
    CASE WHEN Test_Name = 'Citrate' THEN AVERAGE_VALUE ELSE NULL END AS Citrate,
    CASE WHEN Test_Name = 'Sterility' THEN AVERAGE_VALUE ELSE NULL END AS Sterility, 
    -- CASE WHEN Test_Name = 'Sterility (Ph.Eur.)' THEN AVERAGE_VALUE ELSE NULL END AS Sterility_Eur1, --RES-HEAT_TREATMENT_INCUBATION_SOP232774/1
    -- CASE WHEN Test_Name = 'Sterility (Ph.Eur.)' THEN AVERAGE_VALUE ELSE NULL END AS Sterility_Eur2, --RES-HEAT_TREATMENT_INCUBATION_SOP232774/2
    -- CASE WHEN Test_Name = 'Sterility (Ph.Eur, CFR, WHO)' THEN AVERAGE_VALUE ELSE NULL END AS Sterility_Eur_CFR_WHO1, --what is this?
    CASE WHEN Test_Name = 'Bacterial Endotoxins-RES [EU/ml]' THEN AVERAGE_VALUE ELSE NULL END AS Bacterial_Endotoxins_RES,
    CASE WHEN Test_Name = 'Osmolality' THEN AVERAGE_VALUE ELSE NULL END AS Osmolality,
    CASE WHEN Test_Name = 'Particle Contamination >=10 µm [/vial]' THEN AVERAGE_VALUE ELSE NULL END AS Particle_Contamination_10um,
    CASE WHEN Test_Name = 'Particle Contamination >=25 µm [/vial]' THEN AVERAGE_VALUE ELSE NULL END AS Particle_Contamination_25um,
    CASE WHEN Test_Name = 'TAMC Total Count' THEN AVERAGE_VALUE ELSE NULL END AS TAMC_Total_Count,
    CASE WHEN Test_Name = 'Start Aseptic Filling' THEN AVERAGE_VALUE ELSE NULL END AS Start_Aseptic_Filling,
    CASE WHEN Test_Name = 'End Aseptic Filling' THEN AVERAGE_VALUE ELSE NULL END AS End_Aseptic_Filling,
    CASE WHEN Test_Name = 'Start Heat Treatment' THEN AVERAGE_VALUE ELSE NULL END AS Start_Heat_Treatment,
    CASE WHEN Test_Name = 'End Heat Treatment' THEN AVERAGE_VALUE ELSE NULL END AS End_Heat_Treatment,
    CASE WHEN Test_Name = 'Start ???' THEN AVERAGE_VALUE ELSE NULL END AS Start_Heat_Treatment_Incubation, --test missing
    --VN_VISINS_LIQUID_HOLD_TIME [Hold Time] /1 Start hold date/time / 1
    CASE WHEN Test_Name = 'End ???' THEN AVERAGE_VALUE ELSE NULL END AS End_Heat_Treatment_Incubation, -- test missing
    CASE WHEN Test_Name = 'Yield [Albumin / Plasma Equivalent)' THEN AVERAGE_VALUE ELSE NULL END AS Yield_Albumin_Plasma_Equivalent



-- A4B111	End Date	24-APR-2024 14:42:47	NONE	OR_MODA_STERILITY

  FROM
    lot
    JOIN results ON lot.LOT_NUMBER = results.LOT_NUMBER
  -- WHERE T_PH_LOT_NAME LIKE 'A4B082%'
  WHERE T_PH_LOT_NAME LIKE 'A4%'
)
SELECT
  BATCH_ID,
  MAX(Date_of_Manufacture) AS Date_of_Manufacture,
  MAX(Quantity_for_Release) AS Quantity_for_Release,
  MAX(Expiry_Date) AS Expiry_Date,
  MAX(Visual_Control) AS Visual_Control,
  MAX(Visual_Control_Completed) AS Visual_Control_Completed,
  MAX(Protein_Nitrogen_Corrected) AS Protein_Nitrogen_Corrected,
  MAX(Protein_Composition) AS Protein_Composition,
  MAX(Polymers_Aggregates) AS Polymers_Aggregates,
  MAX(Polymers_Aggregates_Protein) AS Polymers_Aggregates_Protein,
  MAX(Prekallikrein_Activator_Activ) AS Prekallikrein_Activator_Activ,
  MAX(Sodium_Content) AS Sodium_Content,
  MAX(Potassium_Content) AS Potassium_Content,
  MAX(Aluminium_Content) AS Aluminium_Content,
  MAX(Haem_Content) AS Haem_Content,
  MAX(pH_Value) AS pH_Value,
  MAX(Heat_Stability) AS Heat_Stability,
  MAX(N_Acetyltryptophane_Content) AS N_Acetyltryptophane_Content,
  MAX(Caprylic_Acid) AS Caprylic_Acid,
  MAX(Citrate) AS Citrate,
  MAX(Sterility) AS Sterility,
  MAX(Bacterial_Endotoxins_RES) AS Bacterial_Endotoxins_RES,
  MAX(Osmolality) AS Osmolality,
  MAX(Particle_Contamination_10um) AS Particle_Contamination_10um,
  MAX(Particle_Contamination_25um) AS Particle_Contamination_25um,
  MAX(Number_of_Filled_Final_Container) AS Number_of_Filled_Final_Container,
  MAX(TAMC_Total_Count) AS TAMC_Total_Count,
  MAX(Start_Aseptic_Filling) AS Start_Aseptic_Filling,
  MAX(End_Aseptic_Filling) AS End_Aseptic_Filling,
  MAX(Start_Heat_Treatment) AS Start_Heat_Treatment,
  MAX(End_Heat_Treatment) AS End_Heat_Treatment,
  MAX(Start_Heat_Treatment_Incubation) AS Start_Heat_Treatment_Incubation,
  MAX(End_Heat_Treatment_Incubation) AS End_Heat_Treatment_Incubation,
  MAX(Yield_Albumin_Plasma_Equivalent) AS Yield_Albumin_Plasma_Equivalent

FROM
  merged_table
GROUP BY
  BATCH_ID;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.get("spark.databricks.clusterUsageTags.clusterId") 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## EL Fractionation IV-V pH & labware values

-- COMMAND ----------

WITH 
lot AS (
  SELECT
    *
  FROM gms_us_lake.gmsgq_glims_lot --JDE: F4108 "Lot Master"
),
results AS (
  SELECT
    *
  FROM gms_us_lake.gmsgq_glims_lot_result --Labware
),
merged_table AS (
  SELECT
    T_PH_LOT_NAME as BATCH_ID,
    NAME as Test_Name,
    AVERAGE_VALUE,
    Units,
    ANALYSIS,
    STAGE,
    SPEC_TYPE,
    results.IN_SPEC,
    -- T_PH_LOT_NAME AS Lot,
    AVERAGE_VALUE,
    CASE WHEN (Test_Name = 'pH Value (Manufacturing)' and STAGE = 'VN_FRAC_I_PH') THEN AVERAGE_VALUE ELSE NULL END AS VN_FRAC_I_PH,
    CASE WHEN (Test_Name = 'pH Value (Manufacturing)' and STAGE = 'VN_FRAC_II_III_PH') THEN AVERAGE_VALUE ELSE NULL END AS VN_FRAC_II_III_PH

  FROM
    lot
    JOIN results ON lot.LOT_NUMBER = results.LOT_NUMBER
  -- WHERE T_PH_LOT_NAME LIKE 'ALB0191%'
  WHERE T_PH_LOT_NAME LIKE 'EL%'
)

SELECT
  BATCH_ID,
  MAX(VN_FRAC_I_PH) AS VN_FRAC_I_PH,
  MAX(VN_FRAC_II_III_PH) AS VN_FRAC_II_III_PH
FROM
  merged_table
GROUP BY
  BATCH_ID;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## VI Disp Date & Lab values

-- COMMAND ----------

WITH 
lot AS (
  SELECT
    *
  FROM gms_us_lake.gmsgq_glims_lot --JDE: F4108 "Lot Master"
),
results AS (
  SELECT
    *
  FROM gms_us_lake.gmsgq_glims_lot_result --Labware
),
merged_table AS (
  SELECT
    T_PH_LOT_NAME as BATCH_ID,
    NAME as Test_Name,
    AVERAGE_VALUE,
    Units,
    ANALYSIS,
    STAGE,
    SPEC_TYPE,
    -- date_format(T_PH_LAST_DISP_ON, 'dd-MMM-yyyy') as VI_PLASMA_RELEASE_DT,
    -- date_format(PRODUCTION_DATE, 'dd-MMM-yyyy') as VI_PRODUCTION_DT,
    T_PH_LAST_DISP_ON as VI_LAST_DISP_ON_DT,
    PRODUCTION_DATE as VI_PRODUCTION_DT,
    results.IN_SPEC
    -- T_PH_LOT_NAME AS Lot
    -- AVERAGE_VALUE,
    -- CASE WHEN (Test_Name = 'pH Value (Manufacturing)' and STAGE = 'VN_FRACT_IV_1_PH') THEN AVERAGE_VALUE ELSE NULL END AS VN_FRACT_IV_1_PH,

-- TAMC Total Count
-- TAMC Mold
-- TYMC Total Count
-- Hepatitis-A Viruses
-- TYMC Mold
-- IgG Nephelometry
-- Protein (Nitrogen corrected) [mg/ml]
-- Parvo B19 Viruses
-- HBs Antigens
-- HIV-1/2 Antibodies
-- Hepatitis-B Viruses
-- Hepatitis-C Viruses
-- HI Viruses 1/2

  FROM
    lot
    JOIN results ON lot.LOT_NUMBER = results.LOT_NUMBER
  -- WHERE T_PH_LOT_NAME LIKE 'ALB0191%'
  WHERE T_PH_LOT_NAME LIKE 'VI%'
)

SELECT
  BATCH_ID,
  VI_LAST_DISP_ON_DT,
  VI_PRODUCTION_DT
  -- DATEDIFF(VI_LAST_DISP_ON_DT, VI_PRODUCTION_DATE) as Inventory_Hold
  -- MAX(VN_FRACT_IV_1_PH) AS VN_FRACT_IV_1_PH,
  -- MAX(VN_FRACT_IV_4_PH) AS VN_FRACT_IV_4_PH,
  -- MAX(VN_FRACT_V_PH) AS VN_FRACT_V_PH


FROM
  merged_table
GROUP BY
  BATCH_ID, VI_LAST_DISP_ON_DT, VI_PRODUCTION_DT
SORT BY BATCH_ID ASC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## labware

-- COMMAND ----------

WITH 
lot AS (
  SELECT
    *
  FROM gms_us_lake.gmsgq_glims_lot --JDE: F4108 "Lot Master"
),
results AS (
  SELECT
    *
  FROM gms_us_lake.gmsgq_glims_lot_result --Labware
),
merged_table AS (
  SELECT
    T_PH_LOT_NAME as BATCH_ID,
    NAME as Test_Name,
    AVERAGE_VALUE,
    Units,
    ANALYSIS,
    STAGE,
    SPEC_TYPE,
    results.IN_SPEC,
    -- T_PH_LOT_NAME AS Lot,
    AVERAGE_VALUE,
    CASE WHEN Test_Name = 'Production Date' THEN AVERAGE_VALUE ELSE NULL END AS Date_of_Manufacture,
    CASE WHEN Test_Name = 'Expiry Date' THEN AVERAGE_VALUE ELSE NULL END AS Expiry_Date,
    CASE WHEN Test_Name = 'Visual Control' THEN AVERAGE_VALUE ELSE NULL END AS Visual_Control,
    CASE WHEN Test_Name = 'Visual Control completed' THEN AVERAGE_VALUE ELSE NULL END AS Visual_Control_Completed,
    CASE WHEN Test_Name = 'Protein (Nitrogen corrected) [mg/ml]' THEN AVERAGE_VALUE ELSE NULL END AS Protein_Nitrogen_Corrected,
    CASE WHEN Test_Name = 'Albumin' THEN AVERAGE_VALUE ELSE NULL END AS Protein_Composition,
    CASE WHEN Test_Name = 'Polymers + Aggregates' THEN AVERAGE_VALUE ELSE NULL END AS Polymers_Aggregates,
    CASE WHEN Test_Name = 'Polymers + Aggregates (Protein%)' THEN AVERAGE_VALUE ELSE NULL END AS Polymers_Aggregates_Protein,
    CASE WHEN Test_Name = 'Prekallikrein Activator Activ.' THEN AVERAGE_VALUE ELSE NULL END AS Prekallikrein_Activator_Activ,
    CASE WHEN Test_Name = 'Sodium [µmol/mL]' THEN AVERAGE_VALUE ELSE NULL END AS Sodium_Content,
    CASE WHEN Test_Name = 'Potassium [µmol/ml]' THEN AVERAGE_VALUE ELSE NULL END AS Potassium_Content,
    CASE WHEN Test_Name = 'Aluminium' THEN AVERAGE_VALUE ELSE NULL END AS Aluminium_Content,
    CASE WHEN Test_Name = 'Haem' THEN AVERAGE_VALUE ELSE NULL END AS Haem_Content,
    CASE WHEN Test_Name = 'pH Value' THEN AVERAGE_VALUE ELSE NULL END AS pH_Value,
    CASE WHEN Test_Name = 'Heat Stability' THEN AVERAGE_VALUE ELSE NULL END AS Heat_Stability,
    CASE WHEN Test_Name = 'N-Acetyltryptophane' THEN AVERAGE_VALUE ELSE NULL END AS N_Acetyltryptophane_Content,
    CASE WHEN Test_Name = 'Caprylic Acid [µmol/ml]' THEN AVERAGE_VALUE ELSE NULL END AS Caprylic_Acid,
    CASE WHEN Test_Name = 'Citrate' THEN AVERAGE_VALUE ELSE NULL END AS Citrate,
    CASE WHEN Test_Name = 'Sterility' THEN AVERAGE_VALUE ELSE NULL END AS Sterility, 
    -- CASE WHEN Test_Name = 'Sterility (Ph.Eur.)' THEN AVERAGE_VALUE ELSE NULL END AS Sterility_Eur1, --RES-HEAT_TREATMENT_INCUBATION_SOP232774/1
    -- CASE WHEN Test_Name = 'Sterility (Ph.Eur.)' THEN AVERAGE_VALUE ELSE NULL END AS Sterility_Eur2, --RES-HEAT_TREATMENT_INCUBATION_SOP232774/2
    -- CASE WHEN Test_Name = 'Sterility (Ph.Eur, CFR, WHO)' THEN AVERAGE_VALUE ELSE NULL END AS Sterility_Eur_CFR_WHO1, --what is this?
    CASE WHEN Test_Name = 'Bacterial Endotoxins-RES [EU/ml]' THEN AVERAGE_VALUE ELSE NULL END AS Bacterial_Endotoxins_RES,
    CASE WHEN Test_Name = 'Osmolality' THEN AVERAGE_VALUE ELSE NULL END AS Osmolality,
    CASE WHEN Test_Name = 'Particle Contamination >=10 µm [/vial]' THEN AVERAGE_VALUE ELSE NULL END AS Particle_Contamination_10um,
    CASE WHEN Test_Name = 'Particle Contamination >=25 µm [/vial]' THEN AVERAGE_VALUE ELSE NULL END AS Particle_Contamination_25um

  FROM
    lot
    JOIN results ON lot.LOT_NUMBER = results.LOT_NUMBER
  -- WHERE T_PH_LOT_NAME LIKE 'A4B082%'
  WHERE T_PH_LOT_NAME LIKE 'A4%'
)
SELECT
  BATCH_ID,
  MAX(Date_of_Manufacture) AS Date_of_Manufacture,
  MAX(Expiry_Date) AS Expiry_Date,
  MAX(Visual_Control) AS Visual_Control,
  MAX(Visual_Control_Completed) AS Visual_Control_Completed,
  MAX(Protein_Nitrogen_Corrected) AS Protein_Nitrogen_Corrected,
  MAX(Protein_Composition) AS Protein_Composition,
  MAX(Polymers_Aggregates) AS Polymers_Aggregates,
  MAX(Polymers_Aggregates_Protein) AS Polymers_Aggregates_Protein,
  MAX(Prekallikrein_Activator_Activ) AS Prekallikrein_Activator_Activ,
  MAX(Sodium_Content) AS Sodium_Content,
  MAX(Potassium_Content) AS Potassium_Content,
  MAX(Aluminium_Content) AS Aluminium_Content,
  MAX(Haem_Content) AS Haem_Content,
  MAX(pH_Value) AS pH_Value,
  MAX(Heat_Stability) AS Heat_Stability,
  MAX(N_Acetyltryptophane_Content) AS N_Acetyltryptophane_Content,
  MAX(Caprylic_Acid) AS Caprylic_Acid,
  MAX(Citrate) AS Citrate,
  MAX(Sterility) AS Sterility,
  MAX(Bacterial_Endotoxins_RES) AS Bacterial_Endotoxins_RES,
  MAX(Osmolality) AS Osmolality,
  MAX(Particle_Contamination_10um) AS Particle_Contamination_10um,
  MAX(Particle_Contamination_25um) AS Particle_Contamination_25um


FROM
  merged_table
GROUP BY
  BATCH_ID;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # labware mehr werte, ähnlich sek

-- COMMAND ----------

WITH 
lot AS (
  SELECT
    *
  FROM gms_us_lake.gmsgq_glims_lot --JDE: F4108 "Lot Master"
),
results AS (
  SELECT
    *
  FROM gms_us_lake.gmsgq_glims_lot_result --Labware
),
merged_table AS (
  SELECT
    T_PH_LOT_NAME as BATCH_ID,
    NAME as Test_Name,
    AVERAGE_VALUE,
    Units,
    ANALYSIS,
    STAGE,
    SPEC_TYPE,
    results.IN_SPEC,
    -- T_PH_LOT_NAME AS Lot,
    AVERAGE_VALUE,
    CASE WHEN Test_Name = 'Production Date' THEN AVERAGE_VALUE ELSE NULL END AS Date_of_Manufacture,
    CASE WHEN Test_Name = 'Expiry Date' THEN AVERAGE_VALUE ELSE NULL END AS Expiry_Date,
    CASE WHEN Test_Name = 'Visual Control' THEN AVERAGE_VALUE ELSE NULL END AS Visual_Control,
    CASE WHEN Test_Name = 'Visual Control completed' THEN AVERAGE_VALUE ELSE NULL END AS Visual_Control_Completed,
    CASE WHEN Test_Name = 'Protein (Nitrogen corrected) [mg/ml]' THEN AVERAGE_VALUE ELSE NULL END AS Protein_Nitrogen_Corrected,
    CASE WHEN Test_Name = 'Albumin' THEN AVERAGE_VALUE ELSE NULL END AS Protein_Composition,
    CASE WHEN Test_Name = 'Polymers + Aggregates' THEN AVERAGE_VALUE ELSE NULL END AS Polymers_Aggregates,
    CASE WHEN Test_Name = 'Polymers + Aggregates (Protein%)' THEN AVERAGE_VALUE ELSE NULL END AS Polymers_Aggregates_Protein,
    CASE WHEN Test_Name = 'Prekallikrein Activator Activ.' THEN AVERAGE_VALUE ELSE NULL END AS Prekallikrein_Activator_Activ,
    CASE WHEN Test_Name = 'Sodium [µmol/mL]' THEN AVERAGE_VALUE ELSE NULL END AS Sodium_Content,
    CASE WHEN Test_Name = 'Potassium [µmol/ml]' THEN AVERAGE_VALUE ELSE NULL END AS Potassium_Content,
    CASE WHEN Test_Name = 'Aluminium' THEN AVERAGE_VALUE ELSE NULL END AS Aluminium_Content,
    CASE WHEN Test_Name = 'Haem' THEN AVERAGE_VALUE ELSE NULL END AS Haem_Content,
    CASE WHEN Test_Name = 'pH Value' THEN AVERAGE_VALUE ELSE NULL END AS pH_Value,
    CASE WHEN Test_Name = 'Heat Stability' THEN AVERAGE_VALUE ELSE NULL END AS Heat_Stability,
    CASE WHEN Test_Name = 'N-Acetyltryptophane' THEN AVERAGE_VALUE ELSE NULL END AS N_Acetyltryptophane_Content,
    CASE WHEN Test_Name = 'Caprylic Acid [µmol/ml]' THEN AVERAGE_VALUE ELSE NULL END AS Caprylic_Acid,
    CASE WHEN Test_Name = 'Citrate' THEN AVERAGE_VALUE ELSE NULL END AS Citrate,
    CASE WHEN Test_Name = 'Sterility' THEN AVERAGE_VALUE ELSE NULL END AS Sterility, 
    -- CASE WHEN Test_Name = 'Sterility (Ph.Eur.)' THEN AVERAGE_VALUE ELSE NULL END AS Sterility_Eur1, --RES-HEAT_TREATMENT_INCUBATION_SOP232774/1
    -- CASE WHEN Test_Name = 'Sterility (Ph.Eur.)' THEN AVERAGE_VALUE ELSE NULL END AS Sterility_Eur2, --RES-HEAT_TREATMENT_INCUBATION_SOP232774/2
    -- CASE WHEN Test_Name = 'Sterility (Ph.Eur, CFR, WHO)' THEN AVERAGE_VALUE ELSE NULL END AS Sterility_Eur_CFR_WHO1, --what is this?
    CASE WHEN Test_Name = 'Bacterial Endotoxins-RES [EU/ml]' THEN AVERAGE_VALUE ELSE NULL END AS Bacterial_Endotoxins_RES,
    CASE WHEN Test_Name = 'Osmolality' THEN AVERAGE_VALUE ELSE NULL END AS Osmolality,
    CASE WHEN Test_Name = 'Particle Contamination >=10 µm [/vial]' THEN AVERAGE_VALUE ELSE NULL END AS Particle_Contamination_10um,
    CASE WHEN Test_Name = 'Particle Contamination >=25 µm [/vial]' THEN AVERAGE_VALUE ELSE NULL END AS Particle_Contamination_25um


    -- CASE WHEN Test_Name = 'End Date' THEN AVERAGE_VALUE ELSE NULL END AS Filling_End_Date, --RES-STERILE_FILLING_BULK_SOP232774/1
    -- CASE WHEN Test_Name = 'Start Date' THEN AVERAGE_VALUE ELSE NULL END AS Filling_Start_Date, --RES-STERILE_FILLING_BULK_SOP232774/2
    -- CASE WHEN Test_Name = 'End Date' THEN AVERAGE_VALUE ELSE NULL END AS Incubation_End_Date,
    -- CASE WHEN Test_Name = 'Start Date' THEN AVERAGE_VALUE ELSE NULL END AS Incubation_Start_Date,--RES-HEAT_TREATMENT_INCUBATION_SOP232774/1
    -- CASE WHEN Test_Name = 'Sterility (Ph.Eur, CFR, WHO)' THEN AVERAGE_VALUE ELSE NULL END AS Sterility_Eur_CFR_WHO2, --RES-HEAT_TREATMENT_INCUBATION_SOP232774/2
    -- CASE WHEN Test_Name = 'Yield [Albumin / Plasma Equivalent)' THEN AVERAGE_VALUE ELSE NULL END AS Yield_Albumin_Plasma_Equivalent,
    -- CASE WHEN Test_Name = 'EM - MIBI - Aseptic Filling' THEN AVERAGE_VALUE ELSE NULL END AS EM_MIBI_Aseptic_Filling,
    -- CASE WHEN Test_Name = 'Events' THEN AVERAGE_VALUE ELSE NULL END AS Events,
    -- CASE WHEN Test_Name = 'Batch Record Print date/time' THEN AVERAGE_VALUE ELSE NULL END AS Batch_Record_Print_dt, --SOP237579/1
    -- CASE WHEN Test_Name = 'Batch Record Print date/time' THEN AVERAGE_VALUE ELSE NULL END AS Batch_Record_Print_dt, --SOP237579/2
    -- CASE WHEN Test_Name = 'MBR Doc-No (Veeva)' THEN AVERAGE_VALUE ELSE NULL END AS MBR_doc_nr4, --4.0
    -- CASE WHEN Test_Name = 'MBR Doc-No (Veeva)' THEN AVERAGE_VALUE ELSE NULL END AS MBR_doc_nr5, --5.0
    -- CASE WHEN Test_Name = 'Quantity for Release' THEN AVERAGE_VALUE ELSE NULL END AS Quantity_for_Release,
    -- CASE WHEN Test_Name = 'Duration Aseptic Filling' THEN AVERAGE_VALUE ELSE NULL END AS Duration_Aseptic_Filling,
    -- CASE WHEN Test_Name = 'Duration Heat Treatment' THEN AVERAGE_VALUE ELSE NULL END AS Duration_Heat_Treatment,
    -- CASE WHEN Test_Name = 'End Aseptic Filling' THEN AVERAGE_VALUE ELSE NULL END AS End_Aseptic_Filling,
    -- CASE WHEN Test_Name = 'End Heat Treatment' THEN AVERAGE_VALUE ELSE NULL END AS End_Heat_Treatment,
    -- CASE WHEN Test_Name = 'Max temperature' THEN AVERAGE_VALUE ELSE NULL END AS Max_Temperature,
    -- CASE WHEN Test_Name = 'Min temperature' THEN AVERAGE_VALUE ELSE NULL END AS Min_Temperature,
    -- CASE WHEN Test_Name = 'Start Aseptic Filling' THEN AVERAGE_VALUE ELSE NULL END AS Start_Aseptic_Filling,
    -- CASE WHEN Test_Name = 'Start Heat Treatment' THEN AVERAGE_VALUE ELSE NULL END AS Start_Heat_Treatment,
    -- CASE WHEN Test_Name = 'Filling Volume' THEN AVERAGE_VALUE ELSE NULL END AS Filling_Volume,
    -- CASE WHEN Test_Name = 'Lot No. & Weight of Used Bulk (1)' THEN AVERAGE_VALUE ELSE NULL END AS Lot_No_Weight_Used_Bulk_1,
    -- CASE WHEN Test_Name = 'Lot Number' THEN AVERAGE_VALUE ELSE NULL END AS Lot_Number,
    -- CASE WHEN Test_Name = 'Number of Filled Final Container' THEN AVERAGE_VALUE ELSE NULL END AS Number_Filled_Final_Container,
    -- CASE WHEN Test_Name = 'Pack Size' THEN AVERAGE_VALUE ELSE NULL END AS Pack_Size,
    -- CASE WHEN Test_Name = 'Preparation Date' THEN AVERAGE_VALUE ELSE NULL END AS Preparation_Date,
    -- CASE WHEN Test_Name = 'Temperature range' THEN AVERAGE_VALUE ELSE NULL END AS Temperature_Range,
    -- CASE WHEN Test_Name = 'TYMC Mold' THEN AVERAGE_VALUE ELSE NULL END AS TYMC_Mold_SOP236144_1,
    -- CASE WHEN Test_Name = 'TYMC Mold' THEN AVERAGE_VALUE ELSE NULL END AS TYMC_Mold_SOP236144_2,
    -- CASE WHEN Test_Name = 'TYMC Total Count' THEN AVERAGE_VALUE ELSE NULL END AS TYMC_Total_Count_SOP236144_1,
    -- CASE WHEN Test_Name = 'TYMC Total Count' THEN AVERAGE_VALUE ELSE NULL END AS TYMC_Total_Count_SOP236144_2,
    -- CASE WHEN Test_Name = 'TAMC Mold' THEN AVERAGE_VALUE ELSE NULL END AS TAMC_Mold_SOP236144_1,
    -- CASE WHEN Test_Name = 'TAMC Mold' THEN AVERAGE_VALUE ELSE NULL END AS TAMC_Mold_SOP243577_1,
    -- CASE WHEN Test_Name = 'TAMC Mold' THEN AVERAGE_VALUE ELSE NULL END AS TAMC_Mold_SOP236144_2,
    -- CASE WHEN Test_Name = 'TAMC Mold' THEN AVERAGE_VALUE ELSE NULL END AS TAMC_Mold_SOP243577_2,
    -- CASE WHEN Test_Name = 'TAMC Total Count' THEN AVERAGE_VALUE ELSE NULL END AS TAMC_Total_Count_SOP236144_1,
    -- CASE WHEN Test_Name = 'TAMC Total Count' THEN AVERAGE_VALUE ELSE NULL END AS TAMC_Total_Count_SOP243577_1,
    -- CASE WHEN Test_Name = 'TAMC Total Count' THEN AVERAGE_VALUE ELSE NULL END AS TAMC_Total_Count_SOP236144_2,
    -- CASE WHEN Test_Name = 'TAMC Total Count' THEN AVERAGE_VALUE ELSE NULL END AS TAMC_Total_Count_SOP243577_2
    -- CASE WHEN Test_Name = 'EM -physical Parameters' THEN AVERAGE_VALUE ELSE NULL END AS EM_Physical_Parameters,
    -- CASE WHEN Test_Name = 'Approval for Packaging' THEN AVERAGE_VALUE ELSE NULL END AS Approval_for_Packaging,
    -- CASE WHEN Test_Name = 'Country Restrictions Drug Substance' THEN AVERAGE_VALUE ELSE NULL END AS Country_Restrictions_Drug_Substance,
    -- CASE WHEN Test_Name = 'Deviation' THEN AVERAGE_VALUE ELSE NULL END AS Deviation,

  FROM
    lot
    JOIN results ON lot.LOT_NUMBER = results.LOT_NUMBER
  -- WHERE T_PH_LOT_NAME LIKE 'A4B082%'
  WHERE T_PH_LOT_NAME LIKE 'A4%'
)
SELECT
  BATCH_ID,
    MAX(Date_of_Manufacture) AS Date_of_Manufacture,
    MAX(Expiry_Date) AS Expiry_Date,
    MAX(Visual_Control) AS Visual_Control,
    MAX(Visual_Control_Completed) AS Visual_Control_Completed,
    MAX(Protein_Nitrogen_Corrected) AS Protein_Nitrogen_Corrected,
    MAX(Protein_Composition) AS Protein_Composition,
    MAX(Polymers_Aggregates) AS Polymers_Aggregates,
    MAX(Polymers_Aggregates_Protein) AS Polymers_Aggregates_Protein,
    MAX(Prekallikrein_Activator_Activ) AS Prekallikrein_Activator_Activ,
    MAX(Sodium_Content) AS Sodium_Content,
    MAX(Potassium_Content) AS Potassium_Content,
    MAX(Aluminium_Content) AS Aluminium_Content,
    MAX(Haem_Content) AS Haem_Content,
    MAX(pH_Value) AS pH_Value,
    MAX(Heat_Stability) AS Heat_Stability,
    MAX(N_Acetyltryptophane_Content) AS N_Acetyltryptophane_Content,
    MAX(Caprylic_Acid) AS Caprylic_Acid,
    MAX(Citrate) AS Citrate,
    MAX(Sterility) AS Sterility,
    MAX(Bacterial_Endotoxins_RES) AS Bacterial_Endotoxins_RES,
    MAX(Osmolality) AS Osmolality,
    MAX(Particle_Contamination_10um) AS Particle_Contamination_10um,
    MAX(Particle_Contamination_25um) AS Particle_Contamination_25um


FROM
  merged_table
GROUP BY
  BATCH_ID;
