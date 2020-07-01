use retail_stg; 

drop table if exists order_rate;
create table retail_stg.order_rate (rid int,orddesc varchar(200),comp_cust varchar(10),siverity int) row format
delimited fields terminated by ',';
load data local inpath '/home/hduser/retailorders/orders_rate.csv' overwrite into table order_rate;



drop table if exists orddetstg;

create table retail_stg.orddetstg (customernumber string,comments string,pagenavigation array<string>,pagenavigationidx array <int>) row format delimited fields terminated by ',' collection items terminated by '$';

load data local inpath '/home/hduser/retailorders/stgdata' overwrite into table retail_stg.orddetstg; 



drop table if exists retail_mart.cust_navigation;
create external table retail_mart.cust_navigation (customernumber string,navigation_pg string,navigation_index
int) row format delimited fields terminated by ',' location '/user/hduser/custmart/';


