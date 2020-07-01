

drop database if exists retail_mart cascade;
create database retail_mart; 

use retail_mart;
set hive.enforce.bucketing = true ;
set map.reduce.tasks = 3;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

create external table custdetpartbuckext (customernumber STRING, customername STRING, contactfullname string, address
struct<addressLine1:string,city:string,state:string,postalCode:bigint,country:string,phone:bigint>,creditlimit
float,checknum string,checkamt int) partitioned by (paymentdate date) clustered by (customernumber) INTO
3 buckets row format delimited fields terminated by '~' collection items terminated by ','
stored as textfile location '/user/hduser/retailorders/custorders/custdetpartbuckext';


insert into table custdetpartbuckext partition(paymentdate)
select customernumber,customername,contactfullname, named_struct('addressLine1' ,address.addressLine1,'city',address.city,'state',address.state,'postalCode',address.postalCode,'country',address.country,'phone',address.phone),creditlimit,checknum,checkamt,paymentdate from retail_stg.custdetstg;


create external table orddetpartbuckext(customernumber STRING, ordernumber STRING, shippeddate date,status string, comments string,productcode string,quantityordered int,priceeach decimal(10,2),orderlinenumber int,productName STRING,productLine STRING, productScale STRING,productVendor STRING,productDescription STRING,quantityInStock int,buyPrice decimal(10,2),MSRP decimal(10,2)) partitioned by (orderdate date) clustered by (customernumber) INTO 3 buckets row format delimited fields terminated by '~' collection items terminated by '$' stored as textfile location '/user/hduser/retailorders/custorders/orddetpartbuckext';


insert into table orddetpartbuckext partition(orderdate) select customernumber,ordernumber, shippeddate,status,comments,productcode,quantityordered ,priceeach ,orderlinenumber ,productName,productLine,
productScale,productVendor,productDescription,quantityInStock,buyPrice,MSRP,orderdate from
retail_stg.orddetstg ;





