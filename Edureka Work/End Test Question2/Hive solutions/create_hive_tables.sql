
use edureka_dw;

drop table if exists edureka_dw.seat_bookings;
CREATE TABLE `seat_bookings`(
  booking_id string,
  `seat_no` int, 
  `row` string, 
  `theatre_id` int, 
  `movie_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'=',', 
  'serialization.format'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';





drop table if exists edureka_dw.theatre;
CREATE TABLE `theatre`(
  `id` int, 
  `row` string, 
  `total_seats_in_row` int, 
  `theatre_name` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'=',', 
  'line.delim'='\n', 
  'serialization.format'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';




drop table if exists edureka_dw.movies;
  CREATE TABLE `movies`(
  `movie_id` string, 
  `movie_name` string, 
  `release_date` string, 
  `end_date` string, 
  `theatre_id` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'colelction.delim'=',', 
  'field.delim'=',', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';



