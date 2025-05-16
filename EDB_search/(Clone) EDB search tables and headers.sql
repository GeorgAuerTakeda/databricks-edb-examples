-- Databricks notebook source
-- MAGIC %md
-- MAGIC -- The following python codes searches for multiple name patterns in the gms_us_lake (~ 2 minutes)
-- MAGIC
-- MAGIC Usage:
-- MAGIC * Enter a database name, enter parts of the table names that you want to search.
-- MAGIC The script now finds (for example) all tables with "JDE" in "gms_us_lake".
-- MAGIC * Now, a column name can be entered (column search). All tables that have this column are then listed.
-- MAGIC Additionally, the last part of the script identifies ALL similar columns in the found tables of step 1.
-- MAGIC The script can be adjusted to find at least X similar strings. It is also possible to exclude column names.
-- MAGIC * Sample values and data format for the found similar columns are extracted and shown.
-- MAGIC For instance, tables with 4 similar characters are put side by side and a sample value is shown for each (~ 10 minutes) 

-- COMMAND ----------

-- MAGIC %md # table search
-- MAGIC given a database name and one or multiple table names, it is possible to search for a certain combination

-- COMMAND ----------

-- MAGIC
-- MAGIC %python
-- MAGIC # Define the database name
-- MAGIC database_name = "gms_us_lake"
-- MAGIC # database_name = "gms_us_hub"
-- MAGIC
-- MAGIC # Define the patterns you're looking for
-- MAGIC # table_patterns = ["*4108*", "*4801*"]
-- MAGIC # table_patterns = ["*veeva*"] --no results so far
-- MAGIC # table_patterns = ["*labware*"]
-- MAGIC # table_patterns = ["*gmsgq_glims*"]
-- MAGIC table_patterns = ["*sap*"]
-- MAGIC # table_patterns = ["*beacon*"]
-- MAGIC # table_patterns = ["*APDS*"]
-- MAGIC # table_patterns = ["*jde*"]
-- MAGIC # table_patterns = ["*lims*"]
-- MAGIC
-- MAGIC # Get the list of tables matching the patterns
-- MAGIC matching_tables = []
-- MAGIC for pattern in table_patterns:
-- MAGIC     tables = spark.sql(f"SHOW TABLES IN {database_name} LIKE '{pattern}'")
-- MAGIC     matching_tables.extend([row.tableName for row in tables.collect()])
-- MAGIC print(matching_tables)
-- MAGIC # Select and display data from the matching tables
-- MAGIC result_dfs = {}
-- MAGIC for table_name in matching_tables:
-- MAGIC     table_data = spark.sql(f"SELECT * FROM {database_name}.{table_name}")
-- MAGIC     result_dfs[table_name] = table_data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(matching_tables)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC all found tables are listed, now one option is to just print out all column names - more convienent is a search function

-- COMMAND ----------

-- MAGIC
-- MAGIC %python
-- MAGIC # Dictionary to store header names for each DataFrame
-- MAGIC header_names_dict = {}
-- MAGIC
-- MAGIC # Loop through the dictionary items
-- MAGIC for db_name, df in result_dfs.items():
-- MAGIC     print(f"DataFrame for {db_name}:")
-- MAGIC
-- MAGIC     # Get the schema (column information) of the DataFrame
-- MAGIC     schema = df.schema
-- MAGIC
-- MAGIC     # Extract header names
-- MAGIC     header_names = [field.name for field in schema]
-- MAGIC
-- MAGIC     # Store the header names in the dictionary
-- MAGIC     header_names_dict[db_name] = header_names
-- MAGIC
-- MAGIC     print("Header Names:", header_names)
-- MAGIC
-- MAGIC     # Perform further comparisons or operations on the header names
-- MAGIC     # You can add your comparison logic or other operations here

-- COMMAND ----------

-- MAGIC %md ## column search
-- MAGIC now, a column name can be entered, all previously found tables are searched if they contain the searchterm

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Searching for a certain term in all found df:
-- MAGIC # searchterm = "LITM"
-- MAGIC searchterm = input("Enter the searchterm: ")
-- MAGIC column_names_dict = {}
-- MAGIC
-- MAGIC # Loop through the dictionary items
-- MAGIC for db_name, df in result_dfs.items():
-- MAGIC     # Get the schema (column information) of the DataFrame
-- MAGIC     schema = df.schema
-- MAGIC
-- MAGIC     # Extract column names
-- MAGIC     column_names = [field.name for field in schema]
-- MAGIC
-- MAGIC     # Store the column names in the dictionary
-- MAGIC     column_names_dict[db_name] = column_names
-- MAGIC
-- MAGIC # Find columns containing the string "LOT"
-- MAGIC matching_lot_columns = []
-- MAGIC
-- MAGIC # Iterate through the dictionary items
-- MAGIC for db_name, column_names in column_names_dict.items():
-- MAGIC     for column in column_names:
-- MAGIC         if searchterm in column:
-- MAGIC             matching_lot_columns.append((db_name, column))
-- MAGIC
-- MAGIC # Print columns containing the search string
-- MAGIC print(f"column Names Containing {searchterm}:")
-- MAGIC for db_name, column in matching_lot_columns:
-- MAGIC     print(f"{db_name}.{column}")

-- COMMAND ----------

select
*
from
gms_us_lake.gmsgq_labware_lims_sin_lot
-- where aliquot_group like "VN RM PR PROTOCOL"
-- gms_us_lake.gmsgq_labware_lims_bp_analysis.group_name

-- COMMAND ----------

-- MAGIC %python
-- MAGIC result_df_test = spark.sql('''
-- MAGIC     SELECT *
-- MAGIC     FROM gms_us_lake.gmsgq_labware_lims_bp_analysis
-- MAGIC     WHERE group_name LIKE "VN RM PR PROTOCOL"
-- MAGIC ''')
-- MAGIC
-- MAGIC # Print the values of the table
-- MAGIC result_df_test.show(n=10)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### search for columns that share a similarity

-- COMMAND ----------

-- MAGIC %md
-- MAGIC additional function: search for columns that share a similarity.
-- MAGIC if there are multiple tables within the EDB that have the same name, they most likely have shared columns.
-- MAGIC
-- MAGIC This could be used to find foreign keys, or to find alternative sources 
-- MAGIC
-- MAGIC also, print out the tables, and sdata types and sample values for the matches.
-- MAGIC
-- MAGIC WARNING: might take several minutes (~10)
-- MAGIC A future version should compare just a few tables, not "the whole EDB"
-- MAGIC
-- MAGIC Example output:
-- MAGIC
-- MAGIC gmsgq_jde_proddta_f4108_adt - gmsgq_jde_proddta_f4801_adt
-- MAGIC
-- MAGIC IOLITM - WALITM
-- MAGIC
-- MAGIC Data Types: StringType() - StringType()
-- MAGIC
-- MAGIC Example Values: 0300058                   - 97104158 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Searching for partially matching header names
-- MAGIC # Data type is also shown for every match
-- MAGIC # Minimum length for partial match is defined here
-- MAGIC min_partial_match_length = 4
-- MAGIC
-- MAGIC # Prompt the user for the excluding_prefix input
-- MAGIC #excluding_prefix = input("Enter the excluding prefix (press Enter to skip): ")
-- MAGIC excluding_prefix = "AUD_"
-- MAGIC
-- MAGIC # Remove leading and trailing whitespace
-- MAGIC excluding_prefix = excluding_prefix.strip()
-- MAGIC
-- MAGIC # Dictionary to store header names, data types, and example values for each DataFrame
-- MAGIC header_info_dict = {}
-- MAGIC
-- MAGIC # Loop through the dictionary items
-- MAGIC for db_name, df in result_dfs.items():
-- MAGIC     # Get the schema (column information) of the DataFrame
-- MAGIC     schema = df.schema
-- MAGIC
-- MAGIC     # Extract header names, data types, and example values
-- MAGIC     header_info = {field.name: (field.dataType, df.select(field.name).first()[0]) for field in schema}
-- MAGIC
-- MAGIC     # Store the header names, data types, and example values in the dictionary
-- MAGIC     header_info_dict[db_name] = header_info
-- MAGIC
-- MAGIC # Create an empty list to store partially matching header name pairs
-- MAGIC partial_matching_headers = []
-- MAGIC
-- MAGIC # Get the database names as a list
-- MAGIC db_names_list = list(header_info_dict.keys())
-- MAGIC
-- MAGIC # Iterate through the dictionary items
-- MAGIC for i, (db1, header_info1) in enumerate(header_info_dict.items()):
-- MAGIC     for db2, header_info2 in list(header_info_dict.items())[i+1:]:  # Compare only with databases that come after the current one
-- MAGIC         for header1, (data_type1, example_value1) in header_info1.items():
-- MAGIC             for header2, (data_type2, example_value2) in header_info2.items():
-- MAGIC                 if (
-- MAGIC                     (excluding_prefix == "" or excluding_prefix not in header1) and
-- MAGIC                     (excluding_prefix == "" or excluding_prefix not in header2) and
-- MAGIC                     len(header1) >= min_partial_match_length and
-- MAGIC                     len(header2) >= min_partial_match_length and
-- MAGIC                     any(header1[i:i+min_partial_match_length] in header2 for i in range(len(header1) - min_partial_match_length + 1))
-- MAGIC                 ):
-- MAGIC                     partial_matching_headers.append((db1, header1, db2, header2, data_type1, data_type2, example_value1, example_value2))
-- MAGIC
-- MAGIC # Print the partial matching header name pairs, their data types, and example values
-- MAGIC print("Partial Matching Header Names (at least 4 characters common substring):")
-- MAGIC for db1, header1, db2, header2, data_type1, data_type2, example_value1, example_value2 in partial_matching_headers:
-- MAGIC     print(f"{db1} - {db2}")
-- MAGIC     print(f"{header1} - {header2}")
-- MAGIC     print(f"Data Types: {data_type1} - {data_type2}")
-- MAGIC     print(f"Example Values: {example_value1} - {example_value2}\n")
