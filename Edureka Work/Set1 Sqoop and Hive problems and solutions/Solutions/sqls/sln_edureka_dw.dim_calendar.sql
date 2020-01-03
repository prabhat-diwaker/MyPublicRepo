---problem statement :
--- calendar data is placed at local edge node path as $ separated flat file. We need to create a managed partitioned table in ORC and load the data from this file
---flat file is present at : /mnt/bigdatapgp/edureka_719925/data_prep/dim_calendar location


--- DDL to create stage hive table

drop table if exists edureka_dw.dim_calendar_stage;
CREATE TABLE `edureka_dw.dim_calendar_stage`(                  
   `cal_dt` date,                                               
   `date_desc` varchar(50),                                     
   `wm_wk_day_nbr` smallint,                                    
   `wm_yr_wk_nbr` int,                                          
   `wm_mth_nbr` int,                                            
   `wm_mth_nm` varchar(50),                                     
   `wm_qtr_nm` varchar(50),                                     
   `wm_half_yr_nbr` int,                                        
   `wm_half_yr_nm` varchar(50),                                 
   `wm_yr_desc` varchar(50),                                    
   `geo_region_cd` string)                                      
 ROW FORMAT DELIMITED                                           
   FIELDS TERMINATED BY '$'                                     
   LINES TERMINATED BY '\n'                                     
 STORED AS INPUTFORMAT                                          
   'org.apache.hadoop.mapred.TextInputFormat'                   
 OUTPUTFORMAT                                                   
   'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' ;




---Load data from Local to stage Hive table
LOAD DATA LOCAL INPATH '/mnt/bigdatapgp/edureka_719925/data_prep/dim_calendar/00*' OVERWRITE INTO TABLE edureka_dw.dim_calendar_stage;



---DDL to create target partitioned hive table in ORC format
drop table if exists edureka_dw.dim_calendar;
CREATE TABLE `edureka_dw.dim_calendar`(
  `cal_dt` date, 
  `date_desc` varchar(50), 
  `wm_wk_day_nbr` smallint, 
  `wm_yr_wk_nbr` int, 
  `wm_mth_nbr` int, 
  `wm_mth_nm` varchar(50), 
  `wm_qtr_nm` varchar(50), 
  `wm_half_yr_nbr` int, 
  `wm_half_yr_nm` varchar(50), 
  `wm_yr_desc` varchar(50),
  `row_insertion_dttm` string
  )
partitioned by 
(
 `geo_region_cd` string
 )
 STORED as ORC;


---Script to load data from stage table to target table 

set hive.exec.dynamic.partition.mode=nonstrict;
insert OVERWRITE table edureka_dw.dim_calendar partition(geo_region_cd)
select 
  cal_dt,
  date_desc,
  wm_wk_day_nbr,
  wm_yr_wk_nbr,
  wm_mth_nbr,
  wm_mth_nm,
  wm_qtr_nm,
  wm_half_yr_nbr,
  wm_half_yr_nm,
  wm_yr_desc,
  current_timestamp() as row_insertion_dttm,
  geo_region_cd
from edureka_dw.dim_calendar_stage;

   


--count check
-- select count(*) from edureka_dw.dim_calendar;
-- 200893


-- select count(*) from edureka_dw.dim_calendar_stage;
-- 200893
