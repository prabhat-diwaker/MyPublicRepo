-- Problem statement
-- we have labuser_database.fact_instk_dly table in mysql and we need to load it to partitioned hive table in ORC format. Target table will be partitioned on bus_dt and op_cmpny_cd columns.


--- DDL to create stage table in Hive

drop table if exists edureka_dw.fact_instk_dly_stage;
CREATE TABLE `edureka_dw.fact_instk_dly_stage`(
  `store_nbr` int, 
  `geo_region_cd` char(2), 
  `mds_fam_id` int, 
  `sys_repl_cd` smallint, 
  `frac_instk_pct` decimal(9,4), 
  `src_rcv_ts` timestamp, 
  `load_userid` varchar(20), 
  `load_ts` timestamp, 
  `upd_ts` timestamp, 
  `upd_userid` varchar(20), 
  `onhand_each_qty` int, 
  `fcst_dmand_each_qty` decimal(9,2), 
  `bus_dt` string, 
  `op_cmpny_cd` varchar(8))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'=',', 
  'line.delim'='\n', 
  'serialization.format'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
TBLPROPERTIES (
  'transient_lastDdlTime'='1571657128')
  ;
  

--Sqoop command to full load data from mysql to existing Hive stage table 

sqoop import --connect "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database" \
--username edu_labuser \
--password  "edureka" \
--query "select * from fact_instk_dly where \$CONDITIONS" \
--target-dir /tmp/edureka_719925/fact_instk_dly \
--delete-target-dir \
--split-by mds_fam_id \
--hive-import \
--hive-overwrite \
--hive-database edureka_dw \
--hive-table fact_instk_dly_stage \
--hive-drop-import-delims \
--fields-terminated-by ',' \
--m 10



--- DDL to create target partitioned fact_instk_dly table in Hive

drop table if exists edureka_dw.fact_instk_dly;
CREATE TABLE `edureka_dw.fact_instk_dly`(                         
  `store_nbr` int,                                             
  `geo_region_cd` char(2),                                     
  `mds_fam_id` int,                                            
  `sys_repl_cd` smallint,                                      
  `frac_instk_pct` decimal(9,4),                               
  `src_rcv_ts` timestamp,                                      
  `load_userid` varchar(20),                                   
  `load_ts` timestamp,                                         
  `upd_ts` timestamp,                                          
  `upd_userid` varchar(20),                                    
  `onhand_each_qty` int,                                       
  `fcst_dmand_each_qty` decimal(9,2), 
  row_insertion_dttm string
  )             
partitioned by   
	(
  `bus_dt` date,                                               
  `op_cmpny_cd` varchar(8)
    )
ROW FORMAT SERDE                                               
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'                  
STORED AS INPUTFORMAT                                          
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'            
OUTPUTFORMAT                                                   
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'            
TBLPROPERTIES (                                                
  'transient_lastDdlTime'='1571318059') ;
  

--Script to load target fact_instk_dly table froms stage table

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table edureka_dw.fact_instk_dly partition (bus_dt,op_cmpny_cd)
select 
  `store_nbr` ,                                             
  `geo_region_cd` ,                                     
  `mds_fam_id` ,                                            
  `sys_repl_cd` ,                                      
  `frac_instk_pct` ,                               
  `src_rcv_ts` ,                                      
  `load_userid` ,                                   
  `load_ts` ,                                         
  `upd_ts` ,                                          
  `upd_userid` ,                                    
  `onhand_each_qty` ,                                       
  `fcst_dmand_each_qty`  ,  
  current_timestamp() as row_insertion_dttm,
  `bus_dt` ,                                               
  `op_cmpny_cd`
 from  
 edureka_dw.fact_instk_dly_stage;
 


------ counts check
-- Mysql
-- select count(*) from labuser_database.fact_instk_dly;
-- 3425327

-- Hive
-- select count(*) from edureka_dw.fact_instk_dly_stage;
-- 3425327

-- hive
-- select count(*) from edureka_dw.fact_instk_dly;
-- 3425327






