
--create Hive table DDL

drop table if exists edureka_dw.dim_item ;
CREATE TABLE `edureka_dw.dim_item`(
  `mds_fam_id` int, 
  `geo_region_cd` char(2), 
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
  `item_type_desc` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='$', 
  'line.delim'='\n', 
  'serialization.format'='$') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='0', 
  'rawDataSize'='0', 
  'totalSize'='241134', 
  'transient_lastDdlTime'='1571402922');


---Create MySQL table DDL

drop table if exists labuser_database.dim_item ;
create table labuser_database.dim_item 
(
	mds_fam_id int,
	geo_region_cd char(2),
	curr_ind tinyint,
	item_nbr int ,
	item_desc_1 varchar(100),
	upc_nbr decimal(18,0),
	cntry_nm varchar(100),
	dept_nbr smallint,
	dept_desc varchar(100),
	op_cmpny_cd varchar(100),
	dept_catg_nbr int,
	dept_catg_desc varchar(100),
	prod_nbr int,
	prod_desc varchar(100),
	brand_id int,
	brand_nm varchar(100),
	brand_ownr_nm varchar(100),
	item_type_desc varchar(100)
) ;


--Load data from Local to Hive table
LOAD DATA LOCAL INPATH '/mnt/bigdatapgp/edureka_719925/data_prep/dim_item' OVERWRITE INTO TABLE edureka_dw.dim_item;


--Load data from Hive table to Mysql Table
sqoop-export -Dmapreduce.job.running.map.limit=30 -Dsqoop.export.records.per.statement=5000 -Dsqoop.export.statements.per.transaction=100000 -Dmapred.job.queue.name=mdsekpidat --num-mappers 10 --connect "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database" --table dim_item  --update-mode allowinsert --hcatalog-table dim_item --hcatalog-database edureka_dw --username edu_labuser --password edureka  
