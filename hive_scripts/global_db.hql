CREATE DATABASE IF NOT EXISTS global_db;

use global_db;

create table IF NOT EXISTS Variant (VariantId INT, MinSize DOUBLE,MaxSize DOUBLE,Size STRING) ROW FORMAT DELIMITED
FIELDS TERMINATED by '\073' stored as textfile
tblproperties ("skip.header.line.count"="1");

LOAD DATA INPATH 'variant.csv' OVERWRITE INTO TABLE Variant;
