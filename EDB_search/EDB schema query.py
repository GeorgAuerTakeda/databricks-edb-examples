# Databricks notebook source
# list the tables in your database.
# spark.catalog.setCurrentDatabase("default")
spark.catalog.setCurrentDatabase("gms_us_hub")
spark.catalog.listTables()

# COMMAND ----------

from pyspark.sql.types import StructType

# Specify your table name
# table_name = "gms_us_hub.txn_inventory_erp_glbl"
table_name = "gms_us_hub.ref_materialbatch_erp_glbl"

# Load the table as a DataFrame and extract the schema
table_df = spark.table(table_name)
table_schema = table_df.schema

# Check that you have loaded the correct schema by printing it
# print(table_schema)

key_names = []

# Loop through the field names in the schema and print them
for field in table_schema.fieldNames():
#   print(field)
  key_names.append(field)

print(key_names)

# COMMAND ----------

# Import the required modules
from pyspark.sql.functions import col
from pyspark.sql.types import StructType

# Set the current database to gms_us_hub
spark.catalog.setCurrentDatabase("gms_us_hub")

# key_of_interest = "INVENTORY_KEY"
# key_of_interest = "BATCH_ID"
# key_of_interest = "MATERIALBATCH_KEY"



# Get a list of tables in the current database
table_list = spark.catalog.listTables()

# Loop through the list of tables and check their schemas
for table in table_list:
    try:
        table_name = table.name
        table_schema = spark.table(table_name).schema
        
        # Check if the table schema contains the INVENTORY_KEY column

        for key_of_interest in key_names:
            if key_of_interest in [column.name for column in table_schema]:
                print(f"Found {key_of_interest} in table {table_name}")
        # if key_of_interest in [column.name for column in table_schema]:
        #     print(f"Found {key_of_interest} in table {table_name}")
    except:
        print(f"No access for table {table_name}")

