


drop table  labuser_database.seat_bookings;
CREATE TABLE labuser_database.seat_bookings(
  booking_id varchar(20),
  `seat_no` int, 
  `row` varchar(20), 
  `theatre_id` int, 
  `booking_date` varchar(20)
  );



  CREATE TABLE labuser_database.theatre(
  `id` int, 
  `row` varchar(20), 
  `total_seats_in_row` int, 
  `theatre_name` varchar(20)
  );


CREATE TABLE labuser_database.movies(
  `movie_id` varchar(20), 
  `movie_name` varchar(20), 
  `release_date` varchar(20), 
  `end_date` varchar(20), 
  `theatre_id` int);


sqoop-export -Dmapreduce.job.running.map.limit=30 -Dsqoop.export.records.per.statement=5000 -Dsqoop.export.statements.per.transaction=100000 --num-mappers 5 --connect "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database" --table seat_bookings  --update-mode allowinsert --hcatalog-table seat_bookings --hcatalog-database edureka_dw --username edu_labuser --password edureka  


sqoop-export -Dmapreduce.job.running.map.limit=30 -Dsqoop.export.records.per.statement=5000 -Dsqoop.export.statements.per.transaction=100000 --num-mappers 5 --connect "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database" --table theatre  --update-mode allowinsert --hcatalog-table theatre --hcatalog-database edureka_dw --username edu_labuser --password edureka  


sqoop-export -Dmapreduce.job.running.map.limit=30 -Dsqoop.export.records.per.statement=5000 -Dsqoop.export.statements.per.transaction=100000 --num-mappers 5 --connect "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database" --table movies  --update-mode allowinsert --hcatalog-table movies --hcatalog-database edureka_dw --username edu_labuser --password edureka  


