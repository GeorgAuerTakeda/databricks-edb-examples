-- Databricks notebook source
/* For finding JDE sources in the EDB, F1 can be pressed while in selecting a field in the JDE enviroment.
For the Branch/Plant field, this "Item Help" information looks like:

***
Alias: MCU
Business View: V4108A (Lot Master)
Table: F4108 (Lot Master)
An alphanumeric code that identifies a separate entity within a business for which you want to track costs. For example, a business unit might be a warehouse location, job, project, work center, branch, or plant.
***

The  resulting table information, for instance F4108 can be searched in the EDB:*/
--show tables in gms_us_lake like '*F4108*'
--Alternatively, one could also search for just the number to receive other similar tables:
-- SHOW TABLES IN gms_us_lake LIKE '*4108*';
SHOW TABLES IN gms_us_lake LIKE '*4801*';
-- SHOW TABLES IN gms_us_lake LIKE '*4102*';
-- SHOW TABLES IN gms_us_lake LIKE '*F5541TB*'; -- traceback, used in lot trace and track

-- COMMAND ----------

/* In a second query, the tables found in the previous step can be selected. The headers are the final information for the data mapping. */
-- select * from gms_us_lake.gmsgq_jde_proddta_f4108_adt
-- select * from gms_us_lake.gmsgq_jde_proddta_f4102_adt
select * from gms_us_lake.gmsgq_jde_proddta_f4801_adt
--select * from gms_us_lake.gmsgq_jde_proddta_f554108t_adt
/*In our case we are looking for the alias "LOTS". This alias is found in column IOLOTS.
Therefore, we now know that the data we are interested in is found in:
gms_us_lake.gmsgq_jde_proddta_f4108_adt.IOLOTS
*/

-- COMMAND ----------

select * from gms_us_lake.gmsgq_jde_proddta_f5534ag

-- COMMAND ----------

-- here we want to find the IOUA04, which is on the lake level
-- on lake level, datetime information can be stored in integer date format
-- the following formula can be used to convert the datetime to ISO_Date
SELECT
    IOUA04 AS INT_Date,
    dateadd( cast( concat( cast(( cast( IOUA04 as int ) div 1000 + 1900 ) as varchar(4) ), '-01-01' ) as date ), cast( IOUA04 as int ) % 1000 - 1 ) 
    AS ISO_Date
FROM
    gms_us_lake.gmsgq_jde_proddta_f4108_adt;
