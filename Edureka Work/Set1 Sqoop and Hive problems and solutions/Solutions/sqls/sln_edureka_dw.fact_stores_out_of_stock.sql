---problem statement : need to identify stores where items went out of stock in a day
--- if fact_instk_dly.onhand_each_qty < fact_scanv_dly.sales_unit_qty then item got out of stock

---DDL to create target partitioned hive table in ORC format
drop table if exists edureka_dw.fact_stores_out_of_stock;
CREATE TABLE `edureka_dw.fact_stores_out_of_stock`(
    `mds_fam_id` int, 
    `store_nbr` int, 
    onhand_each_qty decimal(9,2),
    fcst_dmand_each_qty decimal(9,2),
    store_nm string,
    market_nm string,
    city_nm string,
    `item_desc_1` string, 
    `dept_desc` string, 
    `brand_nm` varchar(50),
    `prod_desc` varchar(50), 
    wm_yr_wk_nbr string,
    wm_qtr_nbr string,
    year string,
    region_nm string,
    buo_area_nm string,
    sales_unit_qty decimal(9,2),
    sales_amt decimal(9,2),
    rtn_amt decimal(9,2)
  )
partitioned by 
(
 `op_cmpny_cd` string,
 bus_dt string
 )
 STORED as ORC;


set hive.auto.convert.join=true;
drop table if exists edureka_dw.instk_with_dim;
create table edureka_dw.instk_with_dim stored as orc as 
select 
    instk.mds_fam_id,
    instk.store_nbr,
    instk.onhand_each_qty,
    instk.fcst_dmand_each_qty,
    instk.op_cmpny_cd,
    instk.bus_dt,
    store.store_nm,
    store.market_nm,
    store.city_nm,
    item.item_desc_1,
    item.dept_desc,
    item.brand_nm,
    item.prod_desc,
    cal.wm_yr_wk_nbr,
    cal.wm_qtr_nm,
    substring(cal.cal_dt,1,3) as year,
    r.region_nm,
    r.buo_area_nm
from (select * from edureka_dw.fact_instk_dly where op_cmpny_cd = 'WMT-US') instk
inner join
(select * from edureka_dw.dim_store where op_cmpny_cd = 'WMT-US') store on instk.store_nbr = store.store_nbr
inner join 
(select * from edureka_dw.dim_item  where geo_region_cd = 'US') item on instk.mds_fam_id=item.mds_fam_id
inner join 
(select * from edureka_dw.dim_calendar where geo_region_cd = 'US')cal on instk.bus_dt = cal.cal_dt
inner join 
(select * from edureka_dw.dim_region where op_cmpny_cd = 'WMT-US') r on store.region_nbr = r.region_nbr
;


set hive.auto.convert.join=false;
insert overwrite table edureka_dw.fact_stores_out_of_stock partition (op_cmpny_cd = 'WMT-US', bus_dt)
select 
    instk.mds_fam_id,
    instk.store_nbr,
    instk.onhand_each_qty,
    instk.fcst_dmand_each_qty,
    instk.store_nm,
    instk.market_nm,
    instk.city_nm,
    instk.item_desc_1,
    instk.dept_desc,
    instk.brand_nm,
    instk.prod_desc,
    instk.wm_yr_wk_nbr,
    instk.wm_qtr_nm,
    instk.year,
    instk.region_nm,
    instk.buo_area_nm,
    scanv.sales_unit_qty,
    scanv.sales_amt,
    scanv.rtn_amt,
    instk.bus_dt
from edureka_dw.instk_with_dim instk
inner join  
(select store_nbr,mds_fam_id,visit_dt,sum(sales_unit_qty) as sales_unit_qty,
    sum(sales_amt) as sales_amt,
    sum(rtn_amt) as rtn_amt
 from edureka_dw.fact_scanv_dly where op_cmpny_cd = 'WMT-US' group by store_nbr,mds_fam_id,visit_dt) scanv
on instk.mds_fam_id=scanv.mds_fam_id
and instk.store_nbr=scanv.store_nbr
and instk.bus_dt = scanv.visit_dt
;
