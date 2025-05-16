# Databricks notebook source
# MAGIC %md # SQL: queries to create new reduced tables

# COMMAND ----------

# The following notebook describes the process of finding tables and columns on the EDB

# COMMAND ----------


# The deloitte list is can be used to identify tables/columns where quantity or qty can be found.
# They could be checked manually, by using a query for each table/column and inspecting the values.

#https://mytakeda.sharepoint.com/:x:/r/sites/GMSGQ_EDB_Team/_layouts/15/doc2.aspx?sourcedoc=%7B22262285-FE55-48BE-811B-B9F5D1CB8988%7D&file=DDM%20Mapping%20Main.xlsm&wdLOR=c9BCAA9CB-7B56-48E5-9F4E-58CCB1BA2E3A&action=default&mobileredirect=true

# using the information of the site, we search for tables with qty in it

# COMMAND ----------

# otherwise, we can also use the python code further down @Results to identify tables
# after using selct * to find all columns, the interesting ones can be picked out and using a certain batch_id we can find out which qty field is of interest.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gms_us_hub.txn_inventory_erp_glbl
# MAGIC where BATCH_ID like "%A4A%"
# MAGIC and PLANT_ID like "%VN1%"

# COMMAND ----------

# now we need to get a cleaner version of this table with only potentially interesting columns

# COMMAND ----------

# MAGIC %sql
# MAGIC -- in this query we define some general columns that help us (batch_id, mat_id, plant_id,..) finding batches, and some columns that sound like they will show us the field (in this case quantity) that we would like to see. if we than limit the results to vienna (where PLANT_ID like "%VN1%") using wildcards, limit them to batches with a certain status (the idea is: if there is a status, it might be still in vienna), and finally limit the search for a certain product (China Albumin = A4A) we might get something where the quantities are not all zero.
# MAGIC -- Ideally we check a certain batch_id in jde (via TER) and use the same batch_id to find the information here. If the quantities in any of the columns match, we have found our table! :)
# MAGIC
# MAGIC select
# MAGIC BATCH_ID,
# MAGIC MAT_ID,
# MAGIC PLANT_ID,
# MAGIC BATCH_STAT_CD,
# MAGIC BLOCK_STOCK_QTY,
# MAGIC RTRN_QTY,
# MAGIC STOCK_QUALITY_INSP_QTY,
# MAGIC STOCK_IN_TRANSIT_L2L_QTY,
# MAGIC STOCK_IN_TRANSIT_P2P_QTY,
# MAGIC STOCK_RSTR_BATCH_TOTAL_QTY,
# MAGIC UNRSTR_USAGE_QTY
# MAGIC from gms_us_hub.txn_inventory_erp_glbl
# MAGIC where PLANT_ID like "%VN1%"
# MAGIC and BATCH_STAT_CD is not null
# MAGIC and BATCH_ID like "%A4A%"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select distinct 
# MAGIC -- batch_stat_cd
# MAGIC -- from gms_us_hub.txn_inventory_erp_glbl where batch_stat_cd = ''
# MAGIC select * from gms_us_hub.txn_inventory_erp_glbl where STOCK_RSTR_BATCH_TOTAL_QTY = 0

# COMMAND ----------

# MAGIC %md # Python: search tables, and search column names within the tables

# COMMAND ----------

# First, you can list the tables in your database.
spark.catalog.listTables()

# Assuming you have a table named 'your_table_name', you can extract the schema information for this table.
your_table_name_schema = spark.table("gms_us_hub.txn_inventory_erp_glbl").schema

# You can then display the schema information.
your_table_name_schema

# Alternatively, you can use SQL queries to extract the schema information.
spark.sql("DESCRIBE your_table_name").show()

# You can also retrieve the schema of all the columns in the table.
spark.sql("SHOW COLUMNS IN your_table_name").show()


# COMMAND ----------

# My python scripts that search for tables and columns within the found tables
# they also show the data type and sample values for the identified tables

# COMMAND ----------

# Get a list of all tables in the "gms_us_hub" schema
all_tables = spark.catalog.listTables("gms_us_hub")
keyword = "inventory"
# Create an empty list to store tables with "inventory" in their names
tables_with_keyword = []

# Loop over all tables and find those with "inventory" in their names
for table in all_tables:
    if keyword in table.name:
        tables_with_keyword.append(table.name)

# COMMAND ----------

#print them
for table_name in tables_with_keyword:
    print(table_name)

# COMMAND ----------

# Create a dictionary to store the found DataFrames, their search terms, and associated column names
found_dataframes = {}

# Loop over tables with keyword in their names and search for columns containing search terms
# search_terms = ["method"]
search_terms = ["qty", "quantity"]

for table_name in tables_with_keyword:
    try:
        # Get the schema for the table
        schema = spark.table(f"gms_us_hub.{table_name}").schema
        
        # Check if any search term column exists (case-insensitive)
        search_term_column_exists = any(any(term in field.name.lower() for term in search_terms) for field in schema)
        
        if search_term_column_exists:
            # Get the DataFrame for the table
            dataframe = spark.sql(f"SELECT * FROM gms_us_hub.{table_name}")
            
            # Find the search term and associated column name for the table
            found_search_term_column = next((field.name for field in schema if any(term in field.name.lower() for term in search_terms)), None)
            
            # Store the DataFrame in the dictionary with table name as key
            found_dataframes[table_name] = {"dataframe": dataframe, "search_term_column": found_search_term_column}
            
    except Exception as e:
        if "AccessDenied" in str(e):
            print(f"Access denied for table {table_name}")
        else:
            print(f"Error for table {table_name}: {str(e)}")




# COMMAND ----------

# Print out the found tables and their associated search term columns
for table_name, data in found_dataframes.items():
    print(f"Found table: {table_name}")
    print(f"Associated search term column: {data['search_term_column']}")
    print()

# COMMAND ----------

# now we can search inside of the columns for specific data: string, int,...
# this might fail due to storage issues if the found tables are too big
# second_search_term = "VN200067 FG QA FINISHING"
second_search_term = 0
 
for table_name, info in found_dataframes.items():
    column = info["search_term_column"]
    print(f"Table: {table_name}, Table: {column}")
    df = found_dataframes[table_name]["dataframe"]
    column_data = df.filter(f"{column} LIKE '%{second_search_term}%'").collect()
    for row in column_data:
        print(f"match in: {row}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gms_us_hub.ref_track_back_erp_glbl
# MAGIC where root_batch_id like "%C3A032AH%"
# MAGIC -- where cmpnt_batch_id like "%VNR223150088%"
# MAGIC and parent_batch_id like "%VNPLX035916%"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gms_us_hub.ref_track_forward_erp_glbl

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gms_us_hub.ref_materialmstr_plant_erp_glbl
