
--DDL to create table in HIve

drop table if exists edureka_dw.dim_region;
CREATE EXTERNAL TABLE IF NOT EXISTS edureka_dw.dim_region (jsonBlob STRING)
LOCATION '/user/edureka_719925/';

SELECT 
 get_json_object(jsonBlob, '$.tag1') AS tag1
FROM DB.dummyTable



add jar /opt/cloudera/parcels/CDH/lib/hive-hcatalog/share/hcatalog/hive-hcatalog-core.jar ;

CREATE EXTERNAL TABLE `dim_region`(
  `subdiv` struct<subdiv_mgr_nm:string,subdiv_nm:string,subdiv_nbr:string> COMMENT 'from deserializer', 
  `region` struct<region_nm:string,region_mgr_nm:string,region_nbr:int> COMMENT 'from deserializer', 
  `recorddetails` struct<op_cmpny_cd:string,upd_userid:string,load_userid:string,load_ts:string,src_rcv_ts:string,upd_ts:string> COMMENT 'from deserializer', 
  `geo_region_cd` string COMMENT 'from deserializer', 
  `buo` struct<buo_area_mgr_nm:string,buo_area_nbr:string,buo_area_nm:string> COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.apache.hive.hcatalog.data.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://nameservice1/user/hive/warehouse/edureka_dw.db/dim_region_parsed'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='false', 
  'last_modified_by'='edureka_719925', 
  'last_modified_time'='1571410783', 
  'numFiles'='0', 
  'numRows'='-1', 
  'rawDataSize'='-1', 
  'totalSize'='0', 
  'transient_lastDdlTime'='1571410783')
  ;
  
  select subdiv.subdiv_mgr_nm,region.region_nbr,geo_region_cd from dim_region_parsed;
