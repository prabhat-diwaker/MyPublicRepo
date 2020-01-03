--- problem statement
--- We have dim_item table in labuser_database db in mysql. We need to import this as a parquet file to a HDFS location and load this data to a partitioned ORC target hive table



--- Sqoop command to import data from mysql dim_item table to HDFS location as parquet file

sqoop import --connect "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database"  \
--username edu_labuser \
--password  "edureka" \
--query "select * from dim_item where \$CONDITIONS" \
--as-parquetfile \
--target-dir /tmp/edureka_719925/dim_item \
--m 1 \
--delete-target-dir


---DDL to create stage hive table with parquet storage

drop table if exists edureka_dw.dim_item_stage ;
CREATE TABLE `edureka_dw.dim_item_stage`(
  `mds_fam_id` int, 
  `geo_region_cd` char(2), 
  `curr_ind` tinyint, 
  `item_nbr` int, 
  `item_desc_1` string, 
  `upc_nbr` string, 
  `cntry_nm` string, 
  `dept_nbr` smallint, 
  `dept_desc` string, 
  `op_cmpny_cd` string, 
  `dept_catg_nbr` int, 
  `dept_catg_desc` string, 
  `prod_nbr` int, 
  `prod_desc` string, 
  `brand_id` int, 
  `brand_nm` string, 
  `brand_ownr_nm` string, 
  `item_type_desc` string)
stored as parquet;


---Load imported data to stage table 
LOAD DATA  INPATH 'hdfs://nameservice1/tmp/edureka_719925/dim_item/' OVERWRITE INTO TABLE edureka_dw.dim_item_stage;



--DDL to create partitioned target hive table with ORC storage

drop table if exists edureka_dw.dim_item ;
CREATE TABLE `edureka_dw.dim_item`(
  `mds_fam_id` int, 
  `curr_ind` tinyint, 
  `item_nbr` int, 
  `item_desc_1` string, 
  `upc_nbr` decimal(18,0), 
  `cntry_nm` string, 
  `dept_nbr` smallint, 
  `dept_desc` string, 
  `op_cmpny_cd` string, 
  `dept_catg_nbr` int, 
  `dept_catg_desc` string, 
  `prod_nbr` int, 
  `prod_desc` string, 
  `brand_id` int, 
  `brand_nm` string, 
  `brand_ownr_nm` string, 
  `item_type_desc` string,
  row_insertion_dttm string
  )
  partitioned by 
  (
  geo_region_cd char(2)
  )
STORED AS ORC;
  


--Script to load data from dim_item_stage to target dim_item table

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table edureka_dw.dim_item partition(geo_region_cd)
select
  mds_fam_id ,
  curr_ind,
  item_nbr,
  item_desc_1,
  cast (upc_nbr as int),
  cntry_nm,
  dept_nbr,
  dept_desc,
  op_cmpny_cd,
  dept_catg_nbr,
  dept_catg_desc,
  prod_nbr,
  prod_desc,
  brand_id,
  brand_nm,
  brand_ownr_nm,
  item_type_desc,
  current_timestamp() as row_insertion_dttm,
  geo_region_cd
from edureka_dw.dim_item_stage;


-- Counts check
-- Mysql
-- select count(*) from labuser_database.dim_item;
-- 72100

-- Hive
-- select count(*) from edureka_dw.dim_item_stage;
-- 72100

-- Hive
-- select count(*) from edureka_dw.dim_item;
-- 72100
