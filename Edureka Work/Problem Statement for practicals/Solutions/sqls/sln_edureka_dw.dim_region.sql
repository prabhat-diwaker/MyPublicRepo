-- Problem STATEMENT
-- We have region data as a json file in a HDFS location. We need to build an external dim_region dimension on this json file.
-- HDFS path of json file : /user/edureka_719925/region_data
-- if data is not present then copy it from local edge node
-- hadoop fs -mkdir /user/edureka_719925/region_data
-- hadoop fs -put -f /mnt/bigdatapgp/edureka_719925/data_prep/dim_region/region.json /user/edureka_719925/region_data/



--DDL to create table in HIve

add jar /opt/cloudera/parcels/CDH/lib/hive-hcatalog/share/hcatalog/hive-hcatalog-core.jar ;


drop table if exists edureka_dw.dim_region_json;
CREATE EXTERNAL TABLE `edureka_dw.dim_region_json`(
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
  'hdfs://nameservice1/user/edureka_719925/region_data/'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='false'
  )
  ;


-- DDL to create target hive table partitioned on op_cmpny_cd

drop table if exists edureka_dw.dim_region;
create table edureka_dw.dim_region
  (
    subdiv_mgr_nm string,
    subdiv_nm string,
    subdiv_nbr int,
    region_nm string,
    region_mgr_nm string,
    region_nbr int,
    upd_userid string,
    load_userid string,
    load_ts string,
    upd_ts string,
    geo_region_cd string,
    buo_area_mgr_nm string,
    buo_area_nbr int,
    buo_area_nm string,
    row_insertion_dttm string
    )
partitioned by 
(
op_cmpny_cd string
)
stored as orc;


set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table edureka_dw.dim_region partition ( op_cmpny_cd)
  select 
  subdiv.subdiv_mgr_nm,
  subdiv.subdiv_nm,
  subdiv.subdiv_nbr,
  region.region_nm,
  region.region_mgr_nm,
  region.region_nbr,
  recorddetails.upd_userid,
  recorddetails.load_userid,
  recorddetails.load_ts,
  recorddetails.upd_ts,
  geo_region_cd,
  buo.buo_area_mgr_nm,
  buo.buo_area_nbr,
  buo.buo_area_nm,
  current_timestamp() as row_insertion_dttm,
  recorddetails.op_cmpny_cd
  from edureka_dw.dim_region_json;


  

-- count check 
-- select count(*) from edureka_dw.dim_region_json;
--148

-- select count(*) from edureka_dw.dim_region;
--148

