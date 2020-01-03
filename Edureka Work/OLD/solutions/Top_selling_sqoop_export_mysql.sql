CREATE TABLE labuser_database.top_selling_category_stores(
	store_nbr int, 
	store_nm varchar(50), 
	dept_catg_desc varchar(200), 
	total_sales double, 
	sales_date date, 
	rnk int
	);



sqoop-export -Dmapreduce.job.running.map.limit=30 -Dsqoop.export.records.per.statement=5000 -Dsqoop.export.statements.per.transaction=100000 --num-mappers 5 --connect "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database" --table top_selling_category_stores  --update-mode allowinsert --hcatalog-table top_selling_category_stores --hcatalog-database edureka_dw --username edu_labuser --password edureka  
