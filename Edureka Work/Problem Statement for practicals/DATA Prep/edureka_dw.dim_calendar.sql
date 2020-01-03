--- DDL to create hive table

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
  `geo_region_cd` string)
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
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='0', 
  'rawDataSize'='0', 
  'totalSize'='11332133', 
  'transient_lastDdlTime'='1571405112')

   
   
   
--Load data from Local to Hive table
LOAD DATA LOCAL INPATH '/mnt/bigdatapgp/edureka_719925/dim_calendar' OVERWRITE INTO TABLE edureka_dw.dim_calendar;

