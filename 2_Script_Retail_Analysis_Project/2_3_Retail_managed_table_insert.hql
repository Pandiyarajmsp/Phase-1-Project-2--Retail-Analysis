use retail_stg;

insert overwrite table custdetstg select customernumber,contactfirstname, concat(contactfirstname, ' ',
contactlastname) , named_struct('addressLine1', addressLine1, 'city', city, 'state', state, 'postalCode', postalCode,
'country', country, 'phone',phone), creditlimit,checknumber,checkamt,paymentdate from custdetails;
---truncate table orddetstg;
