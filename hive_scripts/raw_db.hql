CREATE DATABASE IF NOT EXISTS raw_db ; 

use raw_db; 


create table IF NOT EXISTS Catalogue (ProductColorId INT, GenderLabel STRING, SupplierColorLabel STRING,SeasonLabel STRING) ROW FORMAT DELIMITED
FIELDS TERMINATED by '\073' stored as textfile
tblproperties ("skip.header.line.count"="1"); 


create table IF NOT EXISTS Customer (CustomerId INT, DomainCode STRING, BirthDate TIMESTAMP,Gender STRING,Size DOUBLE) ROW FORMAT DELIMITED
FIELDS TERMINATED by '\073' stored as textfile
tblproperties ("skip.header.line.count"="1"); 


create table IF NOT EXISTS Customer_Order (OrderNumber INT, VariantId INT, CustomerId INT,Quantity INT,UnitPrice DOUBLE,OrderCreationDate STRING) ROW FORMAT DELIMITED
FIELDS TERMINATED by '\073' stored as textfile
tblproperties ("skip.header.line.count"="1"); 


create table IF NOT EXISTS Product_Reference (VariantId INT,ProductColorId INT, ProductId INT) ROW FORMAT DELIMITED
FIELDS TERMINATED by '\073' stored as textfile
tblproperties ("skip.header.line.count"="1"); 


LOAD DATA INPATH 'catalogue.csv' OVERWRITE INTO TABLE Catalogue; 
LOAD DATA INPATH 'customer.csv' OVERWRITE INTO TABLE Customer;
LOAD DATA INPATH 'order.csv' OVERWRITE INTO TABLE Customer_Order;
LOAD DATA INPATH 'product_reference.csv' OVERWRITE INTO TABLE Product_Reference;
