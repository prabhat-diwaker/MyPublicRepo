
sqoop import --connect "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database" \
--username edu_labuser \
--password  "edureka" \
--query "select * from product_1 where \$CONDITIONS" \
--target-dir /tmp/edureka_719925/sqoop_temp2 \
--delete-target-dir \
--hive-import \
--hive-overwrite \
--hive-database edureka_dw \
--hive-table product \
--hive-drop-import-delims \
--fields-terminated-by '|' \
--m 1




sqoop import --connect "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database" \
--username edu_labuser \
--password  "edureka" \
--query "select * from order_1 where \$CONDITIONS" \
--target-dir /tmp/edureka_719925/sqoop_temp3 \
--delete-target-dir \
--hive-import \
--hive-overwrite \
--hive-database edureka_dw \
--hive-table orders \
--hive-drop-import-delims \
--fields-terminated-by '|' \
--m 1



sqoop import --connect "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database" \
--username edu_labuser \
--password  "edureka" \
--query "select * from supplier where \$CONDITIONS" \
--target-dir /tmp/edureka_719925/sqoop_temp4 \
--delete-target-dir \
--hive-import \
--hive-overwrite \
--hive-database edureka_dw \
--hive-table supplier \
--hive-drop-import-delims \
--fields-terminated-by '|' \
--m 1



sqoop import --connect "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database" \
--username edu_labuser \
--password  "edureka" \
--query "select * from customer where \$CONDITIONS" \
--target-dir /tmp/edureka_719925/sqoop_temp4 \
--delete-target-dir \
--hive-import \
--hive-overwrite \
--hive-database edureka_dw \
--hive-table customer \
--hive-drop-import-delims \
--fields-terminated-by '|' \
--m 1