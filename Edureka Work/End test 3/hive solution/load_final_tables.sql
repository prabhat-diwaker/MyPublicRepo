


------profitable product each monthe

drop table if exists edureka_dw.profitable_products_per_month;

create table edureka_dw.profitable_products_per_month stores as orc as
select 
  sale_month,
  final.productname,
  s.companyname as suppliername,
  total_profit,
  round((total_profit/total_buy_price)*100,2) as profit_percent 
from (
  select 
    id,
    productname,
    sale_month,
    total_profit, 
    total_buy_price,dense_rank() over (partition by sale_month order by total_profit desc) as rnk 
    from (
        select 
        id,
        productname,
        sale_month, 
        sum(sell_amount) - sum(buy_price) as total_profit, 
        sum(buy_price) as total_buy_price from
        (
          select 
            b.id,
            b.productname,
            substr(orderdate,1,3)as sale_month,
            Quantity*unitprice as sell_amount, 
            Quantity*productunitprice as buy_price 
          from orders a 
          inner join 
          (select 
              id,
              productname,
              unitprice as productunitprice 
              from product
          ) b 
          on a.productid=b.id
        )a group by id,productname,sale_month
        )a
    )final 
  left join product p on p.id = final.id
  left join supplier s on p.SupplierId=s.id   
  where rnk = 1 and total_profit > 0;


------------product recommendation

drop table if exists edureka_dw.product_recommendations;

create table edureka_dw.product_recommendations stored as orc as
select 
productid,
prod.ProductName,
collect_list(recommended_product_id) as recommended_product_ids,
collect_list(prod1.productname)recommended_product_names 
from 
(
  select 
    t.pid as productid, 
    rid recommended_product_id, 
    row_number() over(partition by t.pid order by t.cnt desc) rn 
  from 
    (
      select 
        o.productid pid,
        p.productid rid, 
        count(*) as cnt 
      from edureka_dw.orders o 
      inner join ( 
            select 
                distinct productid 
                from(
                        select 
                            productid,customer_count,DENSE_RANK() over(order by customer_count desc) as  rnk 
                            from ( 
                                select 
                                    productid,
                                    count(customerid) as customer_count 
                                from orders group by productid
                                )stg
                    )tmp 
                    where rnk <=10
                )top_10_products
                on o.productid=top_10_products.productid
      left join edureka_dw.orders p 
      on o.customerid=p.customerid 
      where o.productid<>p.productid
  group by o.productid, p.productid
  )t
) p
left join product prod on p.productid = prod.id 
left join product prod1 on p.recommended_product_id= prod1.id
where p.rn<=3
group by productid,prod.ProductName;


