
sqoop import --connect "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database" \
--username edu_labuser \
--password  "edureka" \
--query "select * from countries_table where \$CONDITIONS" \
--target-dir /tmp/edureka_719925/sqoop_temp2 \
--delete-target-dir \
--hive-import \
--hive-overwrite \
--hive-database edureka_dw \
--hive-table countries_table \
--hive-drop-import-delims \
--fields-terminated-by '|' \
--m 1




sqoop import --connect "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database" \
--username edu_labuser \
--password  "edureka" \
--query "select * from locations_table where \$CONDITIONS" \
--target-dir /tmp/edureka_719925/sqoop_temp3 \
--delete-target-dir \
--hive-import \
--hive-overwrite \
--hive-database edureka_dw \
--hive-table locations_table \
--hive-drop-import-delims \
--fields-terminated-by '|' \
--m 1


sqoop import --connect "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database" \
--username edu_labuser \
--password  "edureka" \
--query "select * from departments_table where \$CONDITIONS" \
--target-dir /tmp/edureka_719925/sqoop_temp4 \
--delete-target-dir \
--hive-import \
--hive-overwrite \
--hive-database edureka_dw \
--hive-table departments_table \
--hive-drop-import-delims \
--fields-terminated-by '|' \
--m 1


sqoop import --connect "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database" \
--username edu_labuser \
--password  "edureka" \
--query "select * from employees_table where \$CONDITIONS" \
--target-dir /tmp/edureka_719925/sqoop_temp5 \
--delete-target-dir \
--hive-import \
--hive-overwrite \
--hive-database edureka_dw \
--hive-table employees_table \
--hive-drop-import-delims \
--fields-terminated-by '|' \
--m 1


sqoop import --connect "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database" \
--username edu_labuser \
--password  "edureka" \
--query "select * from jobs_table where \$CONDITIONS" \
--target-dir /tmp/edureka_719925/sqoop_temp6 \
--delete-target-dir \
--hive-import \
--hive-overwrite \
--hive-database edureka_dw \
--hive-table jobs_table \
--hive-drop-import-delims \
--fields-terminated-by '|' \
--m 1



sqoop import --connect "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database" \
--username edu_labuser \
--password  "edureka" \
--query "select * from regions_table where \$CONDITIONS" \
--target-dir /tmp/edureka_719925/sqoop_temp8 \
--delete-target-dir \
--hive-import \
--hive-overwrite \
--hive-database edureka_dw \
--hive-table regions_table \
--hive-drop-import-delims \
--fields-terminated-by '|' \
--m 1
