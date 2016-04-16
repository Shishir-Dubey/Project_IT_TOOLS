create database if not exists mirror_db;

use mirror_db;


CREATE TABLE IF NOT EXISTS PRODUCTS (variantid INT, productcolorid INT, productid INT, genderlabel STRING, suppliercolorlabel STRING, size STRING, seasonlabel STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\073'
STORED AS TEXTFILE;


INSERT OVERWRITE TABLE PRODUCTS
SELECT Product_reference.variantid,Product_reference.productcolorid,Product_reference.productid, 
Catalogue.genderlabel, Catalogue.suppliercolorlabel, variant.size, Catalogue.seasonlabel
FROM raw_db.Product_Reference as Product_Reference RIGHT OUTER JOIN raw_db.Catalogue ON (Product_reference.productcolorid=Catalogue.productcolorid)
JOIN global_db.variant as variant ON (Product_reference.variantid=variant.variantid);


CREATE TABLE IF NOT EXISTS PURCHASES (ordernumber INT, variantid INT, customerid INT, quantity INT, unitprice FLOAT, ordercreationdate TIMESTAMP,gender STRING, size INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\073'
STORED AS TEXTFILE;


INSERT OVERWRITE TABLE PURCHASES 
SELECT Customer_order.ordernumber, Customer_order.variantid, Customer_order.customerid, Customer_order.quantity,
Customer_order.unitprice, Customer_order.ordercreationdate, products.genderlabel, products.size
FROM raw_db.Customer_order as Customer_order LEFT OUTER JOIN mirror_db.products as PRODUCTS
ON (Customer_order.variantid = PRODUCTS.variantid);
