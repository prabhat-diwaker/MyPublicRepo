use edureka_dw;

drop table if exists customer;
create table customer (
   Id                   int                ,
   FirstName            varchar(40)        ,
   LastName             varchar(40)        ,
   City                 varchar(40)        ,
   Country              varchar(40)        ,
   Phone                varchar(20)         
) 
row format delimited 
fields terminated by '|'
lines terminated by '\n';




drop table if exists orders;
CREATE TABLE `orders` (
  `id` int,
  `orderdate` varchar(100) ,
  `CustomerId` int ,
  `ProductId` int,
  `UnitPrice` decimal(12,2) ,
  `Quantity` int
)
row format delimited 
fields terminated by '|'
lines terminated by '\n';

drop table if exists product;
create table product (
   Id                   int                ,
   ProductName          varchar(50)        ,
   SupplierId           int                ,
   UnitPrice            decimal(12,2)      ,
   Package              varchar(30)        ,
   IsDiscontinued       char(1)             
)
row format delimited 
fields terminated by '|'
lines terminated by '\n';

drop table if exists supplier;
create table supplier (
   Id                   int         ,
   CompanyName          varchar(40) ,
   ContactName          varchar(50) ,
   City                 varchar(40) ,
   Country              varchar(40) ,
   Phone                varchar(30) ,
   Fax                  varchar(30) 
)
row format delimited 
fields terminated by '|'
lines terminated by '\n';






drop table if exists order_1;
create table order_1  as select o.id,orderdate,CustomerId,ProductId,UnitPrice,Quantity from
orders o 
inner join orderitem b on o.id=b.id


drop table if exists product_1;
create table product_1 as 
select b.id,b.ProductName,b.SupplierId,a.new_unit as UnitPrice,b.Package,b.IsDiscontinued from
(select   productid,min(a.unitprice)  as new_unit from order_1 a inner join product b on a.productid = b.id group by productid) a 
inner join product b
on a.productid=b.id;





-------most profitable product, supplier for each sale_month

select *,  str_to_date(trim(concat(substr(orderdate,8,4),'-',
                case when substr(orderdate,1,3) = 'jan' then '01'
                when substr(orderdate,1,3) = 'feb' then '02'
                when substr(orderdate,1,3) = 'mar' then '03'
                when substr(orderdate,1,3) = 'apr' then '04'
                when substr(orderdate,1,3) = 'may' then '05'
                when substr(orderdate,1,3) = 'jun' then '06'
                when substr(orderdate,1,3) = 'jul' then '07'
                when substr(orderdate,1,3) = 'aug' then '08'
                when substr(orderdate,1,3) = 'sep' then '09'
                when substr(orderdate,1,3) = 'oct' then '10'
                when substr(orderdate,1,3) = 'nov' then '11'
                when substr(orderdate,1,3) = 'dec' then '12'
                end ,'-',
                case when length(trim(substr(orderdate,5,2))) = 1 then  concat('0',trim(substr(orderdate,5,2))) else substr(orderdate,5,2) end )),'%Y-%m-%d') from 
             order_1 limit 5;



select *, substr(orderdate,1,3) from order_1 a inner 




select * from (
select productname,sale_month,total_profit, dense_rank() over (partition by sale_month order by total_profit desc) as rnk from (
select productname,sale_month, sum(sell_amount) - sum(buy_price) as total_profit from
(
select 
  b.productname,
  substr(orderdate,1,3)as sale_month,
  Quantity*unitprice as sell_amount, 
  Quantity*productunitprice as buy_price 
  from orders a 
  inner join 
  (select id,productname,unitprice as productunitprice from product) b 
    on a.productid=b.id
)a group by productname,sale_month
)a
)final where rnk = 1 and total_profit > 0


------------product recommendation
Top 5 recommended products for a product






