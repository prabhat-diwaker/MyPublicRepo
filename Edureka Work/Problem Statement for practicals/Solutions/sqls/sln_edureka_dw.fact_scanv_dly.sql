---- DDL to create external table on existing ORC file


--- if data is not present in the hdfs location then copy data from local edge node
-- hadoop fs -mkdir /user/edureka_719925/scanv_data
-- hadoop fs -put /mnt/bigdatapgp/edureka_719925/data_prep/fact_scanv_dly/000000_0 /user/edureka_719925/scanv_data/

drop table if exists edureka_dw.fact_scanv_dly_stage;
CREATE EXTERNAL TABLE `edureka_dw.fact_scanv_dly_stage`(
  `store_nbr` int, 
  `geo_region_cd` char(2), 
  `scan_id` int, 
  `scan_type` tinyint, 
  `dept_nbr` smallint, 
  `mds_fam_id` int, 
  `upc_nbr` decimal(18,0), 
  `sales_unit_qty` decimal(9,2), 
  `sales_amt` decimal(9,2), 
  `cost_amt` decimal(9,2), 
  `visit_dt` date, 
  `rtn_amt` decimal(9,2), 
  `crncy_cd` char(3), 
  `op_cmpny_cd` varchar(10))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://nameservice1/user/edureka_719925/scanv_data/'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true'
               )
;


---- DDL to create partitioned target fact table from stage table

drop table if exists edureka_dw.fact_scanv_dly;
CREATE TABLE `edureka_dw.fact_scanv_dly`(
  `store_nbr` int, 
  `geo_region_cd` char(2), 
  `scan_id` int, 
  `scan_type` tinyint, 
  `dept_nbr` smallint, 
  `mds_fam_id` int, 
  `upc_nbr` decimal(18,0), 
  `sales_unit_qty` decimal(9,2), 
  `sales_amt` decimal(9,2), 
  `cost_amt` decimal(9,2), 
  `rtn_amt` decimal(9,2), 
  `crncy_cd` char(3),
  row_insertion_dttm string
  )
partitioned by   
(
`visit_dt` date,                                               
`op_cmpny_cd` varchar(8)
 )
 clustered by (geo_region_cd) sorted by (mds_fam_id) into 2 buckets
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true'
               )
;



--- script to load target partitioned fact table from stage table

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.enforce.bucketing=true;
set hive.enforce.sorting=true;

insert overwrite table edureka_dw.fact_scanv_dly partition (visit_dt,op_cmpny_cd) 
select 
store_nbr ,
geo_region_cd , 
scan_id  ,
scan_type ,
dept_nbr ,
mds_fam_id ,
upc_nbr ,
sales_unit_qty ,
sales_amt ,
cost_amt ,
rtn_amt ,
crncy_cd ,
current_timestamp() as row_insertion_dttm,
visit_dt  ,
op_cmpny_cd
from edureka_dw.fact_scanv_dly_stage;

---- count checks
-- select count(*) from edureka_dw.fact_scanv_dly_stage;
-- 8080628

-- select count(*) from edureka_dw.fact_scanv_dly;
-- 8080628

