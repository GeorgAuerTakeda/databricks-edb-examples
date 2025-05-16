# Databricks notebook source
# MAGIC %sql
# MAGIC -- CREATE OR REPLACE TABLE gms_us_alyt.atvie_jde_iss_wo_with_attachment_t AS
# MAGIC WITH
# MAGIC attachment_unbase as (
# MAGIC SELECT 
# MAGIC     wo.wo_num, 
# MAGIC     wo.wo_desc, 
# MAGIC     wo.wo_stat_cd, 
# MAGIC     wo.plant_id, 
# MAGIC     wo.equip_tag_num, 
# MAGIC     wo.asset_num, 
# MAGIC     wo.bu_cd, 
# MAGIC     wo.wo_priority_cd, 
# MAGIC     wo.maint_type_cd,
# MAGIC     CAST(unbase64(att.GDTXFT) AS STRING) AS Attachment,
# MAGIC     wo.actl_wo_end_dt, 
# MAGIC     wo.created_on_dt,
# MAGIC     att.GDTXFT,
# MAGIC     adt.waano
# MAGIC FROM 
# MAGIC     gms_us_hub.txn_ppmaterialview_erp_glbl wo
# MAGIC LEFT JOIN gms_us_lake.gmsgq_jde_proddta_f4801_adt adt ON wo.wo_num = adt.WADOCO
# MAGIC LEFT JOIN gms_us_lake.gmsgq_jde_proddta_f00165 att ON TRIM(wo.wo_num) = TRIM(att.GDTXKY)
# MAGIC WHERE 
# MAGIC     wo.bu_cd LIKE "%VN%"
# MAGIC     AND adt.WAANO IN (766893, 815877)
# MAGIC     AND wo.created_on_dt > 20250101
# MAGIC     AND TRIM(att.GDGTITNM) = "Text01"
# MAGIC )
# MAGIC select 
# MAGIC -- REGEXP_REPLACE(Attachment, '[^\x20-\x7E]', '') as Attachment2
# MAGIC -- ,REGEXP_REPLACE(Attachment, '\u0000', '') as Attachment4
# MAGIC REGEXP_REPLACE(Attachment, '[\u0000]', '') as Attachment_corrected
# MAGIC ,* from attachment_unbase 
# MAGIC -- where Attachment like '%AMERSP%'

# COMMAND ----------

from pyspark.sql.functions import unbase64, col, expr
from pyspark.sql.types import StringType
import base64

# Define a UDF to decode the base64 string and handle the encoding
def decode_base64(encoded_str):
    if encoded_str is not None:
        decoded_bytes = base64.b64decode(encoded_str)
        return decoded_bytes.decode('ISO-8859-1')  # Use the correct encoding here
    return None

# Register the UDF
decode_base64_udf = spark.udf.register("decode_base64", decode_base64, StringType())

# Load the data
df = spark.sql("""
SELECT 
    wo.wo_num, 
    wo.wo_desc, 
    wo.wo_stat_cd, 
    wo.plant_id, 
    wo.equip_tag_num, 
    wo.asset_num, 
    wo.bu_cd, 
    wo.wo_priority_cd, 
    wo.maint_type_cd,
    att.GDTXFT,
    wo.actl_wo_end_dt, 
    wo.created_on_dt,
    adt.waano
FROM 
    gms_us_hub.txn_ppmaterialview_erp_glbl wo
LEFT JOIN gms_us_lake.gmsgq_jde_proddta_f4801_adt adt ON wo.wo_num = adt.WADOCO
LEFT JOIN gms_us_lake.gmsgq_jde_proddta_f00165 att ON TRIM(wo.wo_num) = TRIM(att.GDTXKY)
WHERE 
    wo.bu_cd LIKE '%VN%'
    AND adt.WAANO IN (766893, 815877)
    AND wo.created_on_dt > 20250101
    AND TRIM(att.GDGTITNM) = 'Text01'
""")

# Apply the UDF to decode the base64 string
df = df.withColumn("DecodedAttachment", decode_base64_udf(col("GDTXFT")))

# Filter the results if needed
# df_filtered = df.filter(col("DecodedAttachment").like('%AMERSP%'))

# Display the results
display(df_filtered)

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import base64

df = spark.sql("""      
SELECT 
    wo_num, 
    wo_desc, 
    wo_stat_cd, 
    plant_id, 
    equip_tag_num, 
    asset_num, 
    bu_cd, 
    wo_priority_cd, 
    maint_type_cd,
    att.GDTXFT,
    actl_wo_end_dt, 
    created_on_dt,
    waano
FROM 
    gms_us_hub.txn_ppmaterialview_erp_glbl wo --General data regading work orders
LEFT JOIN gms_us_lake.gmsgq_jde_proddta_f4801_adt adt ON wo.wo_num = adt.WADOCO --this view givs additional info for each work order
LEFT JOIN gms_us_lake.gmsgq_jde_proddta_f00165 att ON Trim(wo.wo_num) = trim(att.GDTXKY) --here you can find the attachments

WHERE 
    bu_cd LIKE "%VN%" --filters for Vienna and Orth
    and adt.WAANO in (766893 , 815877) --filters for orignated by Olivera Dacic and Petra Amertsdorfer (ISS Service Center Employees)
    and created_on_dt > 20250101 --Work Orders created in 2025
    and trim(att.GDGTITNM) = "Text01" --filters attachment for Text only
""")

def decode_base64(encoded_string): #Decoding of the string obtained above
    try:
        return base64.b64decode(encoded_string).decode('utf-8', errors='ignore') 
    except Exception:
        return None

decode_base64_udf = udf(decode_base64, StringType())
decoded_df = df.withColumn("Attachment", decode_base64_udf(df["GDTXFT"]))

decoded_df.createOrReplaceTempView("decoded_view")
# spark.sql("""CREATE OR REPLACE table gms_us_alyt.atvie_jde_iss_wo_with_attachment_t AS SELECT wo_num, 
spark.sql("""SELECT wo_num, 
    wo_desc, 
    wo_stat_cd, 
    plant_id, 
    equip_tag_num, 
    asset_num, 
    bu_cd, 
    wo_priority_cd, 
    maint_type_cd,
    Attachment,
    actl_wo_end_dt, 
    created_on_dt,
    GDTXFT,
    waano 
    FROM decoded_view""")

# display the dataframe
display(decoded_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE OR REPLACE TABLE gms_us_alyt.atvie_jde_iss_wo_with_attachment_t AS
# MAGIC WITH
# MAGIC attachment_unbase as (
# MAGIC SELECT 
# MAGIC     adt.WALOTN,
# MAGIC     wo.wo_num, 
# MAGIC     wo.wo_desc, 
# MAGIC     wo.wo_stat_cd, 
# MAGIC     wo.plant_id, 
# MAGIC     wo.equip_tag_num, 
# MAGIC     wo.asset_num, 
# MAGIC     wo.bu_cd, 
# MAGIC     wo.wo_priority_cd, 
# MAGIC     wo.maint_type_cd,
# MAGIC     -- CAST(unbase64(att.GDTXFT) AS STRING) AS Attachment,
# MAGIC     REGEXP_REPLACE(CAST(unbase64(att.GDTXFT) AS STRING), '[\u0000]', '') as Attachment,
# MAGIC     wo.actl_wo_end_dt, 
# MAGIC     wo.created_on_dt,
# MAGIC     att.GDTXFT,
# MAGIC     att.GDGTITNM,
# MAGIC     att.GDTXKY,
# MAGIC     adt.waano
# MAGIC FROM 
# MAGIC     gms_us_hub.txn_ppmaterialview_erp_glbl wo
# MAGIC LEFT JOIN gms_us_lake.gmsgq_jde_proddta_f4801_adt adt ON wo.wo_num = adt.WADOCO
# MAGIC LEFT JOIN gms_us_lake.gmsgq_jde_proddta_f00165 att ON TRIM(wo.wo_num) = TRIM(att.GDTXKY)
# MAGIC WHERE 
# MAGIC     wo.bu_cd LIKE "%VN%"
# MAGIC     -- AND adt.WAANO IN (766893, 815877)
# MAGIC     -- AND wo.created_on_dt > 20240101
# MAGIC     -- AND TRIM(att.GDGTITNM) = "Text01"
# MAGIC )
# MAGIC
# MAGIC -- batch_attachment as (
# MAGIC -- select 
# MAGIC -- trim(matview.batch_id) as batch_id
# MAGIC -- ,matview.wo_num
# MAGIC -- ,matview.wo_desc
# MAGIC -- ,matview.wo_stat_cd
# MAGIC -- ,matview.plant_id
# MAGIC -- ,attachment_unbase.Attachment_corrected
# MAGIC -- -- ,*
# MAGIC -- from gms_us_hub.txn_ppmaterialview_erp_glbl matview
# MAGIC -- join attachment_unbase
# MAGIC -- on matview.wo_num = attachment_unbase.wo_num
# MAGIC -- )
# MAGIC
# MAGIC -- SELECT * FROM batch_attachment
# MAGIC -- WHERE batch_id is not null
# MAGIC -- and batch_id not like ''
# MAGIC -- WHERE batch_id like 'A4B%'
# MAGIC -- where Attachment like '%AMERSP%'
# MAGIC
# MAGIC select * from attachment_unbase
# MAGIC -- where Attachment LIKE '%hold%'
# MAGIC -- WHERE GDTXKY ilike '%A4B%'

# COMMAND ----------

# MAGIC %sql
# MAGIC with Attachment as (
# MAGIC   select 
# MAGIC   GDTXKY,
# MAGIC   SUBSTRING_INDEX(TRIM(gdtxky), '|', -1) AS batch_id,
# MAGIC   REGEXP_REPLACE(CAST(unbase64(GDTXFT) AS STRING), '[\u0000]', '') as Attachment,
# MAGIC   GDUSER,
# MAGIC   GDGTITNM,
# MAGIC   AUD_CRT_DTS,
# MAGIC   ACTIVE_FLAG,
# MAGIC   aud_file_nm
# MAGIC   from gms_us_lake.gmsgq_jde_proddta_f00165
# MAGIC )
# MAGIC select 
# MAGIC   GDTXKY,
# MAGIC   batch_id,
# MAGIC   case when Attachment ilike '%L�ndereinschr�nkung%' then true else false end as country_limitation,
# MAGIC   case when Attachment ilike '%non deviation hold%' then true else false end as non_deviation_hold,
# MAGIC   Attachment,
# MAGIC   GDUSER,
# MAGIC   GDGTITNM,
# MAGIC   AUD_CRT_DTS,
# MAGIC   ACTIVE_FLAG,
# MAGIC   aud_file_nm,
# MAGIC   *
# MAGIC from Attachment
# MAGIC where 1 = 1
# MAGIC and AUD_CRT_DTS > '2025-01-01'
# MAGIC -- and gduser ilike '%SONNLEA   %'
# MAGIC and (batch_id like 'A4%' or batch_id like 'A1%' or batch_id like 'B5%')
# MAGIC -- and (Attachment ilike '%non deviation hold%' or Attachment like '%L�ndereinschr�nkung%')

# COMMAND ----------

# MAGIC %sql
# MAGIC with Attachment as (
# MAGIC   select 
# MAGIC   GDTXKY
# MAGIC   ,REGEXP_REPLACE(CAST(unbase64(GDTXFT) AS STRING), '[\u0000]', '') as Attachment
# MAGIC   ,GDUSER
# MAGIC   ,GDGTITNM
# MAGIC   ,AUD_CRT_DTS
# MAGIC   ,ACTIVE_FLAG
# MAGIC   ,aud_file_nm
# MAGIC   -- ,GDTXKY as wo_num
# MAGIC   -- ,* 
# MAGIC   from gms_us_lake.gmsgq_jde_proddta_f00165
# MAGIC )
# MAGIC select * from Attachment
# MAGIC where 1 = 1
# MAGIC and (Attachment ilike '%non deviation hold%' or Attachment like '%L�ndereinschr�nkung%')
# MAGIC and AUD_CRT_DTS > '2024-01-01'
# MAGIC -- and gduser ilike '%SONNLEA   %'
# MAGIC -- and GDTXKY ilike '%A4B%'
