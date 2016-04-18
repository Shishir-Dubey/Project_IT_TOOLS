----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---GENDER_COUNT---
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

create database if not exists core_db;

use core_db;

CREATE TABLE IF NOT EXISTS Gender_count (customerid int, gender string, COUNT_Gender int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\073'
STORED AS TEXTFILE;



INSERT OVERWRITE TABLE gender_count
select customerid, gender, count(gender) as count_gender
from mirror_db.purchases
group by customerid, gender
order by customerid;
	
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----GENDER_MAX-----
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

create database if not exists core_db;

use core_db;

CREATE TABLE IF NOT EXISTS Gender_max (customerid int, Dominant_gender string, Max_gender int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\073'
STORED AS TEXTFILE;



INSERT OVERWRITE TABLE gender_max
select t1.customerid, t1.gender, t1.count_gender
from core_db.gender_count t1
inner join
(
  select customerid, max(count_gender) as max_count
  from core_db.gender_count
  group by customerid
) t2
  on t1.customerid = t2.customerid
  and t1.count_gender = t2.max_count;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-----SIZE_COUNT-----
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS size_count (customerid int, size string, count_size string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\073'
STORED AS TEXTFILE;



INSERT OVERWRITE TABLE size_count
select customerid, size, count(size) as count_size
from mirror_db.purchases
group by customerid,size
order by customerid;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----SIZE_MAX----
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

create database if not exists core_db;

use core_db;

CREATE TABLE IF NOT EXISTS size_max (customerid int, Dominant_size string, Max_size int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\073'
STORED AS TEXTFILE;




INSERT OVERWRITE TABLE size_max
select t1.customerid, t1.size, t1.count_size
from core_db.size_count t1
inner join
(
  select customerid, max(count_size) as max_count
  from core_db.size_count
  group by customerid
) t2
  on t1.customerid = t2.customerid
  and t1.count_size = t2.max_count;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----last_purchase-----
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS last_purchase(customerid int,last_purchase timestamp);

INSERT INTO TABLE last_purchase
SELECT customerid,MAX(ordercreationdate) as ordercreationdate
FROM mirror_db.purchases
GROUP BY customerid;


-------------------------------------------------------------------------
------Product Final-----
--------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS PRODUCT_Final (productid int,TOTAL_AMOUNT_SOLD DOUBLE,TOTAL_QUANTITY_SOLD INT,NUMBER_OF_DISTINCT_CUSTOMERS INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\073'
STORED AS TEXTFILE;

insert overwrite table PRODUCT_Final
select p.productid,sum(p.unitprice),count(p.quantity),count(distinct(p.customerid))
from
( select b.productid,a.unitprice,a.quantity,a.customerid 
  from mirror_db.purchases a left outer join mirror_db.products b 
  on(a.variantid =b.variantid)
) p
group by p.productid;


------------------------------------------------------------------------------
------------------Customer Final--------
------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS CUSTOMER_FINAL (Customerid INT, Dominant_Gender STRING, Dominant_Size STRING, Last_Purchase_Date TIMESTAMP)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\073'
STORED AS TEXTFILE;

insert overwrite table CUSTOMER_FINAL
select table1.customerid, table1.Dominant_Gender,table2.Dominant_SIZE,table3.last_purchase
from core_db.gender_max as table1 join core_db.size_max as table2 on (table1.customerid = table2.customerid)
join core_db.last_purchase as table3 on (table3.customerid=table1.customerid);
