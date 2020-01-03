--- DDL to create table in Hive

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
  `fcst_dmand_each_qty` decimal(9,2) ,                                                                       
  `bus_dt` date,                                               
  `op_cmpny_cd` varchar(8))                                    
ROW FORMAT SERDE                                               
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'                  
STORED AS INPUTFORMAT                                          
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'            
OUTPUTFORMAT                                                   
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'            
TBLPROPERTIES (                                                
  'transient_lastDdlTime'='1571318059') ;
  
--Load data from Local to Hive table
LOAD DATA LOCAL INPATH '/mnt/bigdatapgp/edureka_719925/fact_instk_dly' OVERWRITE INTO TABLE edureka_dw.fact_instk_dly;

--total count : 3425327


--- DDL to create table in MYSQL

drop table if exists labuser_database.fact_instk_dly;
CREATE TABLE labuser_database.fact_instk_dly(                         
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
  `fcst_dmand_each_qty` decimal(9,2) ,                                                                       
  `bus_dt` date,                                               
  `op_cmpny_cd` varchar(8)) ;
  
  
  
--Load data from Hive table to Mysql Table
sqoop-export -Dmapreduce.job.running.map.limit=30 -Dsqoop.export.records.per.statement=5000 -Dsqoop.export.statements.per.transaction=100000 -Dmapred.job.queue.name=mdsekpidat --num-mappers 20 --connect "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database" --table fact_instk_dly  --update-mode allowinsert --hcatalog-table fact_instk_dly --hcatalog-database edureka_dw --username edu_labuser --password edureka  

-- count : 3425327


--Sqoop command to load data from mysql to Hive table 

sqoop import --connect "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database" \
--username edu_labuser \
--password  "edureka" \
--query "select * from fact_instk_dly where \$CONDITIONS" \
--target-dir /tmp/edureka_719925 \
--delete-target-dir \
--split-by mds_fam_id \
--hive-import \
--hive-overwrite \
--hive-database edureka_dw \
--hive-table fact_instk_dly \
--hive-drop-import-delims \
--m 10


