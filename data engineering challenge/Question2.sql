
-------------get list of top selling product in terms of total quantities ordered
drop table if exists #topproducts;
select distinct productid 
into #topproducts from(
select productid,total_order,DENSE_RANK() over(order by total_order desc) as  rnk from ( 
select productid,sum(cast(quantity as int)) as total_order from dbo.ordertable group by productid
)a
)tmp where rnk <=10 ;	


-----------get all orderIDs as no of baskets
declare @noBasket int;
select @noBasket = count(distinct orderid) from dbo.ordertable where productid in (select productid from #topproducts);


-----------calculate support,confidence and liftRatio

select * from
(
	select 
	productA,
	productB,
	occurences, 
	round (cast(occurences as float) / cast (@noBasket as float),2) as Support,
	round (cast(occurences as float) / cast (prodA_freq.product_freq as float),2) as Confidence,
	round( (cast(occurences as float) / cast (@noBasket as float) ) / ( cast(prodA_freq.product_freq as float) / cast (@noBasket as float) * cast(prodB_freq.product_freq as float) / cast (@noBasket as float)),2) as LiftRatio
	from
	( 
	select 
		productA,
		productB,
		count(*) as occurences 
		from
		(
			select 
			distinct 
			top_sellers.orderid, 
			top_sellers.productid as productA,
			order_table.productid productB  
			from
			(select * from dbo.ordertable where productid in (select productid from #topproducts)) top_sellers
			inner join 
			dbo.ordertable order_table 
			on 
				top_sellers.orderid = order_table.orderid and 
				top_sellers.ProductID<order_table.ProductID
			where top_sellers.ProductID<> order_table.ProductID
		)tmp group by productA,productB
	)tmp1 
	left join
	(select count( distinct orderid) as product_freq,productid from dbo.ordertable where productid in(select productid from #topproducts) group by productid) prodA_freq
	on 
		tmp1.productA = prodA_freq.productid
	left join
	(select count( distinct orderid) as product_freq,productid from dbo.ordertable group by productid) prodB_freq
	on 
		tmp1.productB = prodB_freq.productid
)
stg 
--where --support >= 0.2 and
	-- Confidence >= 0.6
	--and LiftRatio > 1
order by occurences desc
;
