--- Problem Statement 
-- We have labuser_database.dim_store table in  Mysql and we need to load it to HIVE ORC partitioned table. Target table is partitioned on op_cmpny_cd column.


-- DDL to create stage table in Hive
drop table if exists edureka_dw.dim_store_stage;
CREATE TABLE `edureka_dw.dim_store_stage`(
  `store_nbr` int, 
  `geo_region_cd` string, 
  `store_nm` string, 
  `region_nbr` int, 
  `region_nm` string, 
  `market_nbr` int, 
  `market_nm` string, 
  `city_nm` string, 
  `state_prov_cd` string, 
  `load_ts` string, 
  `op_cmpny_cd` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='$', 
  'line.delim'='\n' )
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true');
  
  

--sqoop command to import data from Mysql to existing stage hive table

sqoop import --connect "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database" \
--username edu_labuser \
--password  "edureka" \
--query "select * from dim_store where \$CONDITIONS" \
--target-dir /tmp/edureka_719925/dim_store \
--delete-target-dir \
--hive-import \
--hive-overwrite \
--hive-database edureka_dw \
--hive-table dim_store_stage \
--hive-drop-import-delims \
--fields-terminated-by '$' \
--m 1


--- DDL to create partitioned target hive table

drop table if exists edureka_dw.dim_store;
CREATE TABLE `edureka_dw.dim_store`(
  `store_nbr` int, 
  `geo_region_cd` string, 
  `store_nm` string, 
  `region_nbr` int, 
  `region_nm` string, 
  `market_nbr` int, 
  `market_nm` string, 
  `city_nm` string, 
  `state_prov_cd` string, 
  `load_ts` string, 
  row_insertion_dttm string )
  partitioned by 
  (
  `op_cmpny_cd` string
  )
STORED AS ORC;
   
  

--Script to load target dim_store table from stage table

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table edureka_dw.dim_store partition (op_cmpny_cd)
select 
store_nbr,
geo_region_cd,
store_nm,
region_nbr,
region_nm,
market_nbr,
market_nm,
city_nm,
state_prov_cd,
load_ts,
current_timestamp() as row_insertion_dttm,
op_cmpny_cd
from edureka_dw.dim_store_stage;
