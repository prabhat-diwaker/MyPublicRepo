
--- DDL to create hive table
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
  `op_cmpny_cd` string)
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
  'totalSize'='611472', 
  'transient_lastDdlTime'='1571403665')
   
   
--- DDL to create MySQL table   
drop table if exists labuser_database.dim_store;
CREATE TABLE labuser_database.dim_store(                         
   `store_nbr` int,                                                
   `geo_region_cd` varchar(100),                                         
   `store_nm` varchar(100),                                              
   `region_nbr` int,                                               
   `region_nm` varchar(100),                                             
   `market_nbr` int,                                               
   `market_nm` varchar(100),                                             
   `city_nm` varchar(100),                                               
   `state_prov_cd` varchar(100),                                         
   `load_ts` varchar(100),                                               
   `op_cmpny_cd` varchar(100)
   );


--Load data from Local to Hive table
LOAD DATA LOCAL INPATH '/mnt/bigdatapgp/edureka_719925/data_prep/dim_store' OVERWRITE INTO TABLE edureka_dw.dim_store;


--Load data from Hive table to Mysql Table
sqoop-export -Dmapreduce.job.running.map.limit=30 -Dsqoop.export.records.per.statement=5000 -Dsqoop.export.statements.per.transaction=100000 -Dmapred.job.queue.name=mdsekpidat --num-mappers 10 --connect "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database" --table dim_store  --update-mode allowinsert --hcatalog-table dim_store --hcatalog-database edureka_dw --username edu_labuser --password edureka  
