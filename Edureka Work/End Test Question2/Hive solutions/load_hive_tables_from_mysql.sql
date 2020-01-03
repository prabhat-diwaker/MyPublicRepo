
sqoop import --connect "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database" \
--username edu_labuser \
--password  "edureka" \
--query "select * from theatre where \$CONDITIONS" \
--target-dir /tmp/edureka_719925/sqoop_temp2 \
--delete-target-dir \
--hive-import \
--hive-overwrite \
--hive-database edureka_dw \
--hive-table theatre \
--hive-drop-import-delims \
--fields-terminated-by ',' \
--m 1



sqoop import --connect "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database" \
--username edu_labuser \
--password  "edureka" \
--query "select * from movies where \$CONDITIONS" \
--target-dir /tmp/edureka_719925/sqoop_temp2 \
--delete-target-dir \
--hive-import \
--hive-overwrite \
--hive-database edureka_dw \
--hive-table movies \
--hive-drop-import-delims \
--fields-terminated-by ',' \
--m 1



sqoop import --connect "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database" \
--username edu_labuser \
--password  "edureka" \
--query "select * from seat_bookings where \$CONDITIONS" \
--target-dir /tmp/edureka_719925/sqoop_temp2 \
--delete-target-dir \
--hive-import \
--hive-overwrite \
--hive-database edureka_dw \
--hive-table seat_bookings \
--hive-drop-import-delims \
--fields-terminated-by ',' \
--m 1