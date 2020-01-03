
#************************ import mysql table to a hive table
drop table if exists fact_sales;
create table fact_sales(
item_id int,
store_nbr int,
sales array<int>
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
LINES TERMINATED BY '\n';   

sqoop import --connect "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database" \
--username edu_labuser \
--password  "edureka" \
--query "select * from fact_sales where \$CONDITIONS" \
--target-dir /tmp/edureka_719925/fact_sales \
--delete-target-dir \
--hive-import \
--hive-overwrite \
--hive-database edureka_dw \
--hive-table fact_sales \
--hive-drop-import-delims \
--fields-terminated-by '|' \
--m 1

#************************* load sales amount table to a hive table from a local file

drop table if exists edureka_dw.sales_amount;
create table edureka_dw.sales_amount(
sales_id int,
sales_amount float,
transaction_date string
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';   


LOAD DATA LOCAL INPATH '/mnt/bigdatapgp/edureka_719925/data_prep/sales_amount/sales_amount.txt' INTO TABLE edureka_dw.sales_amount;

#********************** join fact sales with dim_item and store to get category and store name. After that explode the sales ids for each item and store combination.

drop table if exists store_item_category_sales;
create table store_item_category_sales as
select 
	store_nbr,
	store_nm,
	dept_catg_desc,
	sales_ids 
	from 
	(
	select 
		a.*,
		b.item_desc_1,
		b.dept_catg_desc,
		c.store_nm 
	from fact_sales a 
	inner join dim_item b on a.item_id=b.mds_fam_id
	inner join dim_store c on a.store_nbr= c.store_nbr
	)sag
	 
	lateral view explode(sales) temp_table as sales_ids;


#********************** get sum of sales amount for each store,category and date combination and take the most selling item department category in a store on a date

drop table if exists top_selling_category_stores;
create table top_selling_category_stores as
select * from (
select 
	store_nbr,
	store_nm,
	a.dept_catg_desc,
	total_sales,
	sales_date,
	row_number() over(partition by store_nbr,store_nm, sales_date order by total_sales desc) as rnk
from
	(
		select  
			store_nbr,
			store_nm, 
			a.dept_catg_desc,
			to_date(transaction_date) as sales_date,
			round(sum(b.sales_amount),3) as total_sales 
		from store_item_category_sales a inner join sales_amount b
		on a.sales_ids = b.sales_id
		group by store_nbr,store_nm, a.dept_catg_desc,to_date(transaction_date) 
	)a
)b where rnk=1;
