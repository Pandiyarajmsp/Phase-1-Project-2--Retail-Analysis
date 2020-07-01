use retail_stg; 

insert overwrite table retail_mart.cust_navigation select customernumber,pgnavigation as pagenavig
,pgnavigationidx as pagenavigindex from retail_stg.orddetstg lateral view posexplode(pagenavigation) exploded_data1 as x, pgnavigation lateral view posexplode(pagenavigationidx) exploded_data2 as y, pgnavigationidx where x=y;

drop table if exists cust_frustration_level;
create external table retail_mart.cust_frustration_level (customernumber string,total_siverity int,frustration_level string) row format delimited fields terminated by ',' location '/user/hduser/custmartfrustration/';


insert overwrite table retail_mart.cust_frustration_level select customernumber,total_siverity,case when total_siverity between -10 and -3 then 'highly frustrated' when total_siverity between -2 and -1 then 'low frustrated' when total_siverity = 0 then 'neutral' when total_siverity between 1 and 2 then 'happy' when total_siverity between 3 and 10 then 'overwhelming' else 'unknown' end as customer_frustration_level from (select customernumber,sum(siverity) as total_siverity from ( select
o.customernumber,o.comments,r.orddesc,siverity from retail_stg.orddetstg o left outer join order_rate r where o.comments like concat('%',r.orddesc,'%')) temp1 group by customernumber) temp2;


