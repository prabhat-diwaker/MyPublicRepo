drop table if exists labuser_database.fact_stores_out_of_stock;
CREATE TABLE labuser_database.fact_stores_out_of_stock(
    mds_fam_id int, 
    store_nbr int, 
    onhand_each_qty decimal(9,2),
    fcst_dmand_each_qty decimal(9,2),
    store_nm varchar(50),
    market_nm varchar(50),
    city_nm varchar(50),
    item_desc_1 varchar(50), 
    dept_desc varchar(50), 
    brand_nm varchar(50),
    prod_desc varchar(50), 
    wm_yr_wk_nbr varchar(50),
    wm_qtr_nbr varchar(50),
    year varchar(50),
    region_nm varchar(50),
    buo_area_nm varchar(50),
    sales_unit_qty decimal(9,2),
    sales_amt decimal(9,2),
    rtn_amt decimal(9,2),
    op_cmpny_cd varchar(50),
    bus_dt varchar(50)
  );



sqoop-export -Dmapreduce.job.running.map.limit=30 -Dsqoop.export.records.per.statement=5000 -Dsqoop.export.statements.per.transaction=100000 --num-mappers 5 --connect "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database" --table fact_stores_out_of_stock  --update-mode allowinsert --hcatalog-table fact_stores_out_of_stock --hcatalog-database edureka_dw --username edu_labuser --password edureka  
