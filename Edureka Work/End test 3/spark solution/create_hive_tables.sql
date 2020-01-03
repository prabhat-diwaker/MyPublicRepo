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




