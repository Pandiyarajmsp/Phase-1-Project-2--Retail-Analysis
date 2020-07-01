
drop database if exists retail_stg cascade;
create database retail_stg; 
use retail_stg;

create table custdetails(customerNumber string,customerName string,contactFirstName string,contactLastName
string,phone bigint,addressLine1 string,city string,state string,postalCode bigint,country
string,salesRepEmployeeNumber string,creditLimit float,checknumber string,paymentdate date, checkamt float)
row format delimited fields terminated by '~'
location '/home/hduser/retailorders/custdetails/2016-10';


create table custdetstg(customernumber STRING, customername STRING, contactfullname string, address
struct<addressLine1:string,city:string,state:string,postalCode:bigint,country:string,phone:bigint>, creditlimit
float,checknum string,checkamt int,paymentdate date)
row format delimited fields terminated by '~' collection items terminated by ','
stored as textfile;





create  table orddetstg (customerNumber string, ordernumber string,orderdate date, shippeddate
date,status string, comments string, quantityordered int,priceeach decimal(10,2),orderlinenumber int,
productcode string, productName STRING,productLine STRING, productScale STRING,productVendor
STRING,productDescription STRING,quantityInStock int,buyPrice decimal(10,2),MSRP decimal(10,2))
row format delimited fields terminated by '~'
location '/home/hduser/retailorders/orderdetails/2016-10/';


