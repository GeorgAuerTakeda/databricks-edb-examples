-- Databricks notebook source
select trade_nm from gms_us_hub.ref_brandmstr_ibp_glbl
where global_brand_nm like 'NATRIUM CHLORATUM'
