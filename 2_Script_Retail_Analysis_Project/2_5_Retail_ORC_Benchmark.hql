

use retail_mart;

drop table if exists custordfinal;

create external table custordfinal (
customernumber STRING, customername STRING, contactfullname string, addressLine1 string,city string,state
string,country string,phone bigint,creditlimit float,checknum string,checkamt int,ordernumber STRING,
shippeddate date,status string, comments string,productcode string,quantityordered int,priceeach
decimal(10,2),orderlinenumber
int,productName
STRING,productLine
STRING,productScale
STRING,productVendor STRING,productDescription STRING,quantityInStock int,buyPrice decimal(10,2),MSRP
decimal(10,2),orderdate date) row format delimited fields terminated by '~' stored as orcfile
location '/user/hduser/custorders/custordfinal';

insert into table custordfinal
select cd.customernumber,cd.customername, cd.contactfullname,
cd.address.addressLine1,cd.address.city,cd.address.state,cd.address.country,cd.address.phone
,cd.creditlimit,cd.checknum,cd.checkamt,
o.ordernumber,
o.shippeddate,o.status,o.comments,o.productcode,o.quantityordered ,o.priceeach
,o.orderlinenumber ,o.productName ,o.productLine,
productScale,o.productVendor,o.productDescription,o.quantityInStock,o.buyPrice,o.MSRP,o.orderdate from
custdetpartbuckext cd inner join orddetpartbuckext o on cd.customernumber=o.customernumber ;

drop table if exists custordpartfinal;

create external table custordpartfinal (
customernumber STRING, customername STRING, contactfullname string, addressLine1 string,city string,state
string,country string,phone bigint,creditlimit float,checknum string,checkamt int,ordernumber STRING,
shippeddate date,status string, comments string,productcode string,quantityordered int,priceeach
decimal(10,2),orderlinenumber int,productName STRING,productLine STRING,productScale
STRING,productVendor STRING,productDescription STRING,quantityInStock int,buyPrice decimal(10,2),MSRP
decimal(10,2),orderdate date) partitioned by (paymentdate date) row format delimited fields terminated by '~'
stored as textfile
location '/user/hduser/custorders/custordpartfinal';


create index idx_custordpartfinal_phone on table custordpartfinal(phone) AS
'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD;


insert into table custordpartfinal partition(paymentdate) select cd.customernumber,cd.customername,cd.contactfullname,cd.address.addressLine1,cd.address.city,cd.address.state,cd.address.country,cd.address.phone,cd.creditlimit,cd.checknum,cd.checkamt,o.ordernumber, o.shippeddate,o.status,o.comments,o.productcode,o.quantityordered ,o.priceeach,o.orderlinenumber ,o.productName ,o.productLine,
productScale,o.productVendor,o.productDescription,o.quantityInStock,o.buyPrice,o.MSRP,o.orderdate,cd.paymentdate
from custdetpartbuckext cd inner join orddetpartbuckext o on cd.customernumber=o.customernumber ;

