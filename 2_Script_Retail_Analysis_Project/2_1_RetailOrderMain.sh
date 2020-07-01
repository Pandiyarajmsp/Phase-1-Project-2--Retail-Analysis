Datetimelog=$(date +"%Y%m%d%H%M%S")
Datetime=$(date +"%Y-%m-%d %H:%M:%S")
StartDatetime=$(date +"%Y-%m-%d %H:%M:%S")

echo "#-----------------------------Function to handle Error entry in log files--------------------------------------------"
Error_Writer() {
		if [ $1 -ne 0 ] && [ "$4" != "file" ] ;
		then
			cat "$2" >> "$3"
			echo "Error in automation flow ,Please check log for more info"
			exit 1
		fi
		if [ $1 -ne 0 ] && [ "$4" = "file" ] ;
		then
			echo $(date +"%y/%m/%d %H:%M:%S")" Error in ""$5" >> "$3"
			echo "Error in automation flow ,Please check log for more info"
			exit 1
		fi
 };



echo "#------------------------------Error and log file direcorty creation---------------------------------------------------"
mkdir -p /home/hduser/2_Script_Retail_Analysis_Project/LogDir/

Logfilename="/home/hduser/2_Script_Retail_Analysis_Project/LogDir/RetailAnalytics.log"
Errorfilename="/home/hduser/2_Script_Retail_Analysis_Project/LogDir/RetailAnalyticsError.log"
echo "" >$Errorfilename

echo "#-----------------------------RetailAnalytics project data load started Good Luck--------------------------------------"
echo "#-----------------------------Mysql table creation and data import started---------------------------------------------"

echo $(date +"%y/%m/%d %H:%M:%S")"-------------------RetailAnalytics project data load started Good Luck---------------------" >>"$Logfilename"


echo $(date +"%y/%m/%d %H:%M:%S")" MySql :Table creation and dataload for ordersproduct_ORIG started " >>"$Logfilename"
	mysql -u root -proot -D mysql -e "source /home/hduser/retailorders/ordersproduct_ORIG.sql;" 
	Error_Writer $? "$Errorfilename" "$Logfilename" "file" "ordersproduct_ORIG.sql"
echo $(date +"%y/%m/%d %H:%M:%S")" MySql :Table creation and dataload for ordersproduct_ORIG completed " >>"$Logfilename"

echo $(date +"%y/%m/%d %H:%M:%S")" MySql :Table creation and dataload for custpayments_ORIG started " >>"$Logfilename"
	mysql -u root -proot -D mysql -e "source /home/hduser/retailorders/custpayments_ORIG.sql;"
	Error_Writer $? "$Errorfilename" "$Logfilename" "file" "custpayments_ORIG.sql"
echo $(date +"%y/%m/%d %H:%M:%S")" MySql :Table creation and dataload for custpayments_ORIG completed " >>"$Logfilename"

echo $(date +"%y/%m/%d %H:%M:%S")" MySql :Table creation and dataload for empoffice started " >>"$Logfilename"
	mysql -u root -proot -D mysql -e "source /home/hduser/retailorders/empoffice.sql;"
	Error_Writer $? "$Errorfilename" "$Logfilename" "file" "empoffice.sql"
echo $(date +"%y/%m/%d %H:%M:%S")" MySql :Table creation and dataload for empoffice completed " >>"$Logfilename"

#source /home/hduser/retailorders/ordersproduct_ORIG.sql
#source /home/hduser/retailorders/custpayments_ORIG.sql
#source /home/hduser/retailorders/empoffice.sql
echo "#-----------------------------Mysql table creation and data import completed---------------------------------------------"


echo "#-----------------------------Hive Internal table creation started------------------------------------------------------"

echo $(date +"%y/%m/%d %H:%M:%S")" Hive  : custdetails,custdetstg,orddetstg table creation under retail_stg database started " >>"$Logfilename"
hive -f "/home/hduser/2_Script_Retail_Analysis_Project/2_2_Retail_Internal_table.hql"
Error_Writer "$?" "$Errorfilename" "$Logfilename" "file" "2_2_Retail_Internal_table.hql"
echo $(date +"%y/%m/%d %H:%M:%S")" Hive  : custdetails,custdetstg,orddetstg table creation under retail_stg database completed " >>"$Logfilename"

echo "#-----------------------------Hive Internal table creation completed----------------------------------------------------"



echo "#-----------------------------Sqoop Import Started for employees -------------------------------------------------------"

echo $(date +"%y/%m/%d %H:%M:%S")" Sqoop import :employees table data import started " >>"$Logfilename"
sqoop --options-file /home/hduser/retailorders/empofficeoption --password-file /user/hduser/retailorders/root.password -table employees -m 2 --delete-target-dir --target-dir /home/hduser/retailorders/employees/ --fields-terminated-by '~' --lines-terminated-by '\n' --direct 2> "$Errorfilename"
Error_Writer "$?" "$Errorfilename" "$Logfilename" "Shell"
echo $(date +"%y/%m/%d %H:%M:%S")" Sqoop import :employees table data import completed " >>"$Logfilename"

echo "#-----------------------------Sqoop Import completed for employees -------------------------------------------------------"


echo "#-----------------------------Sqoop Import Started for offices table -------------------------------------------------------"
echo $(date +"%y/%m/%d %H:%M:%S")" Sqoop import :offices table data import started " >>"$Logfilename"
sqoop --options-file /home/hduser/retailorders/empofficeoption --password-file /user/hduser/retailorders/root.password -table offices -m 1 --delete-target-dir --target-dir /home/hduser/retailorders/offices/ --fields-terminated-by '~' --lines-terminated-by '\n' 2> "$Errorfilename"
Error_Writer "$?" "$Errorfilename" "$Logfilename" "Shell"
echo $(date +"%y/%m/%d %H:%M:%S")" Sqoop import :offices table data import completed " >>"$Logfilename"
echo "#-----------------------------Sqoop Import completed for offices table -------------------------------------------------------"


echo "#-----------------------------Sqoop Import started for customer join with payment -------------------------------------------------------"
echo $(date +"%y/%m/%d %H:%M:%S")" Sqoop import :customer and payment table data imported to Hdfs started " >>"$Logfilename"
sqoop --options-file /home/hduser/retailorders/custoption --password-file /user/hduser/retailorders/root.password --boundary-query " select min(customerNumber),max(customerNumber) from payments " --query 'select c.customerNumber,upper(c.customerName),c.contactFirstName,c.contactLastName,c.phone,c.addressLine1,c.city,c.state,c.postalCode,c.country ,c.salesRepEmployeeNumber,c.creditLimit ,p.checknumber,p.paymentdate,p.amount
from customers c inner join payments p on c.customernumber=p.customernumber where $CONDITIONS' \
--split-by c.customernumber --delete-target-dir --target-dir /home/hduser/retailorders/custdetails/2016-10/ --null-string 'NA' \
--direct --num-mappers 2 --fields-terminated-by '~' --lines-terminated-by '\n' 2> "$Errorfilename"
Error_Writer "$?" "$Errorfilename" "$Logfilename" "Shell"
echo $(date +"%y/%m/%d %H:%M:%S")" Sqoop import :customer and payment table data imported to Hdfs completed " >>"$Logfilename"
echo "#-----------------------------Sqoop Import completed for customer join with payment -------------------------------------------------------"


echo "#-----------------------------Sqoop Import Started for orders join orderdetails join products -------------------------------------------------------"
echo $(date +"%y/%m/%d %H:%M:%S")" Sqoop import :orders , orderdetails and products table data import to Hdfs started " >>"$Logfilename"
sqoop --options-file /home/hduser/retailorders/ordersoption --password-file /user/hduser/retailorders/root.password --boundary-query "select min(customerNumber),max(customerNumber) from orders" \
--query 'select o.customernumber,o.ordernumber,o.orderdate,o.shippeddate,o.status,o.comments,od.productcode,od.quantityOrdered,od.priceeach,
od.orderlinenumber,p.productCode,p.productName,p.productLine,p.productScale,p.productVendor,p.productDescription,p.quantityInStock,p.buyPrice,
p.MSRP from orders o inner join orderdetails od on o.ordernumber=od.ordernumber inner join products p on od.productCode=p.productCode where $CONDITIONS' \
--split-by o.customernumber --delete-target-dir --target-dir /home/hduser/retailorders/orderdetails/2016-10/ --null-string 'NA' \
--direct --num-mappers 4 --fields-terminated-by '~' --lines-terminated-by '\n' 2> "$Errorfilename"
Error_Writer "$?" "$Errorfilename" "$Logfilename" "Shell"
echo $(date +"%y/%m/%d %H:%M:%S")" Sqoop import :orders , orderdetails and products table data import to Hdfs completed " >>"$Logfilename"
echo "#-----------------------------Sqoop Import Started for orders join orderdetails join products -------------------------------------------------------"


echo "#-------------------------------Sqoop import -all completed ------------------------------------------------------------------"




echo "#-------------------------------Hive Internal table data load started------------------------------------------------------"

echo $(date +"%y/%m/%d %H:%M:%S")" Hive  : custdetstg data load  started " >>"$Logfilename"
hive -f "/home/hduser/2_Script_Retail_Analysis_Project/2_3_Retail_managed_table_insert.hql"
Error_Writer "$?" "$Errorfilename" "$Logfilename" "file" "2_3_Retail_managed_table_insert.hql"
echo $(date +"%y/%m/%d %H:%M:%S")" Hive  : custdetstg data load completed " >>"$Logfilename"

echo "#-------------------------------Hive Internal table data load completed----------------------------------------------------"



#-------------------------------running through Beeline example ------------------------------------------------
#Error Handling Example to show 
#echo $(date +"%y/%m/%d %H:%M:%S")" Hive database creation throw Beeline " >>"$Logfilename"
#export BEELINE_PREFIX="jdbc:hive2://localhost:10000 -n root -p root"
#export FILE_PATH="/home/hduser/2_Script_Retail_Analysis_Project/beelinetests.hql"
#beeline -u $BEELINE_PREFIX -f $FILE_PATH 2>"$Errorfilename"
#Error_Writer "$?" "$Errorfilename" "$Logfilename" "Shell"
#echo $(date +"%y/%m/%d %H:%M:%S")" Hive database creation throw Beeline" >>"$Logfilename"

#-------------------------------running through Beeline example ------------------------------------------------

echo "#--------------------Hive External table custdet and Orddetpartbuckext folder deletion with file started-----------------------------------------"
hadoop fs -rm r /user/hduser/retailorders/custorders/custdetpartbuckext
hadoop fs -rm r /user/hduser/retailorders/custorders/orddetpartbuckext
echo "#--------------------Hive External table custdet and Orddetpartbuckext folder deletion with file completed----------------------------------------"


echo "#-------------------------------Hive External table partition creation and data load started------------------------------------------------------"

echo $(date +"%y/%m/%d %H:%M:%S")" Hive  : Text file custdetpartbuckext,orddetpartbuckext table creation and partion load started " >>"$Logfilename"
hive -f "/home/hduser/2_Script_Retail_Analysis_Project/2_4_Retail_External_table_withPartitionandBucket.hql"
Error_Writer "$?" "$Errorfilename" "$Logfilename" "file" "2_4_Retail_External_table_withPartitionandBucket.hql"
echo $(date +"%y/%m/%d %H:%M:%S")" Hive  : Text file custdetpartbuckext,orddetpartbuckext table creation and partion load completed " >>"$Logfilename"

echo "#-------------------------------Hive External table partition creation and data load completed----------------------------------------------------"


echo "#--------------------Hive External table custordpartfinal and custordfinal folder deletion with file started-----------------------------------------"
hadoop fs -rm r /user/hduser/custorders/custordpartfinal
hadoop fs -rm r /user/hduser/custorders/custordfinal
echo "#--------------------Hive External table custordpartfinal and custordfinal folder deletion with file completed----------------------------------------"


echo "#-------------------------------Hive External ORC table partition creation and data load started------------------------------------------------------"

echo $(date +"%y/%m/%d %H:%M:%S")" Hive  : ORC custdetpartbuckext,orddetpartbuckext table creation and partion load started " >>"$Logfilename"
hive -f "/home/hduser/2_Script_Retail_Analysis_Project/2_5_Retail_ORC_Benchmark.hql"
Error_Writer "$?" "$Errorfilename" "$Logfilename" "file" "2_5_Retail_ORC_Benchmark.hql"
echo $(date +"%y/%m/%d %H:%M:%S")" Hive  : ORC custdetpartbuckext,orddetpartbuckext table creation and partion load completed " >>"$Logfilename"

echo "#-------------------------------Hive External ORC table partition creation and data load completed----------------------------------------------------"



echo "#--------------------Hive External table cust_navigation folder deletion with file started-----------------------------------------"
hadoop fs -rm r /user/hduser/custmart/
echo "#--------------------Hive External table cust_navigation folder deletion with file completed----------------------------------------"


echo "#--------------------Hive External table cust_navigation usecase related table load started-----------------------------------------"
echo $(date +"%y/%m/%d %H:%M:%S")" Hive  : order_rate,orddetstg,cust_navigation table creation and data load started " >>"$Logfilename"
hive -f "/home/hduser/2_Script_Retail_Analysis_Project/2_6_Website_view_usecause.hql"
Error_Writer "$?" "$Errorfilename" "$Logfilename" "file" "2_6_Website_view_usecause.hql"
echo $(date +"%y/%m/%d %H:%M:%S")" Hive  : order_rate,orddetstg,cust_navigation table creation and data load completed " >>"$Logfilename"
echo "#--------------------Hive External table cust_navigation usecase related table load completed----------------------------------------"


hadoop fs -rm r /user/hduser/custmartfrustration/


echo "#--------------------Hive External table cust_frustration_level table load using posexplode function started-----------------------------------------"

echo $(date +"%y/%m/%d %H:%M:%S")" Hive  : cust_frustration_level table creation and data load started " >>"$Logfilename"
hive -f "/home/hduser/2_Script_Retail_Analysis_Project/2_7_Website_view_Usecase_posexplode.hql"
Error_Writer "$?" "$Errorfilename" "$Logfilename" "file" "2_7_Website_view_Usecase_posexplode.hql"
echo $(date +"%y/%m/%d %H:%M:%S")" Hive  : cust_frustration_level table creation and data load completed " >>"$Logfilename"

echo "#--------------------Hive External table cust_frustration_level table load using posexplode function completed--------------------------------"



echo "#--------------------MYSQL customer_reports.customer_frustration_level  table creation started--------------------------------------------------"

echo $(date +"%y/%m/%d %H:%M:%S")" MYSQL : customer_reports.customer_frustration_level table creation started " >>"$Logfilename"
mysql -u root -proot -D custdb -e "Drop database if exists customer_reports;
create database customer_reports;
drop table if exists customer_frustration_level;
CREATE TABLE customer_reports.customer_frustration_level ( customernumber varchar(200), total_siverity float,frustration_level varchar(100) );" 2> "$Errorfilename"
Error_Writer "$?" "$Errorfilename" "$Logfilename" "shell" 
echo $(date +"%y/%m/%d %H:%M:%S")" MYSQL : customer_reports.customer_frustration_level table  completed " >>"$Logfilename"

echo "#--------------------MYSQL customer_reports.customer_frustration_level table creation completed-------------------------------------------------"


echo "#--------------------Sqoop export : customer_reports.customer_frustration_level table data load started-----------------------------------------"

echo $(date +"%y/%m/%d %H:%M:%S")" Hive  : cust_frustration_level table creation and data load started " >>"$Logfilename"
sqoop export --connect jdbc:mysql://localhost/customer_reports --username root --password root --table customer_frustration_level --export-dir /user/hduser/custmartfrustration/ ; > "$Errorfilename"
Error_Writer "$?" "$Errorfilename" "$Logfilename" "shell" 
echo $(date +"%y/%m/%d %H:%M:%S")" Hive  : cust_frustration_level table creation and data load completed " >>"$Logfilename"
echo "#--------------------Sqoop export : customer_reports.customer_frustration_level table data load completed-----------------------------------------"

EndDatetime=$(date +"%Y-%m-%d %H:%M:%S")
echo "--------project executed successfully in" $((($(date -ud "$EndDatetime" +'%s') - $(date -ud "$StartDatetime" +'%s'))/60)) "Minute -------">>"$Logfilename"

echo "-----------------project executed successfully in" $((($(date -ud "$EndDatetime" +'%s') - $(date -ud "$StartDatetime" +'%s'))/60)) "Minute --------------"  

