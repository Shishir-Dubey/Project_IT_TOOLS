#!/bin/bash

###################################################################
# Daily zip files are suposed to be droped in DailyData folder    #
# (another system or the marketing dep is in charge of that part  #
###################################################################

# achives zip file
cp ~/it_tools/daily_data/dwh_*.tar.gz ~/it_tools/archives/
# unzip files
tar -xvzf ~/it_tools/daily_data/dwh_*.tar.gz

## Moving and renaming the files 
mv Catalogue_*.csv ~/it_tools/daily_data/catalogue.csv
mv Customer_*.csv ~/it_tools/daily_data/customer.csv
mv Order_*.csv ~/it_tools/daily_data/order.csv
mv Product_Reference_*.csv ~/it_tools/daily_data/product_reference.csv

# Changes the "," into "." in order.csv 
sed "s/,/./g" ~/it_tools/daily_data/order.csv > temp.csv && mv temp.csv ~/it_tools/daily_data/order.csv

# now that the preprocessing is done
# we can shove the csv files up HDFS
hadoop fs -moveFromLocal ~/it_tools/daily_data/product_reference.csv
hadoop fs -moveFromLocal ~/it_tools/daily_data/customer.csv
hadoop fs -moveFromLocal ~/it_tools/daily_data/order.csv
hadoop fs -moveFromLocal ~/it_tools/daily_data/catalogue.csv
# should be made only once
hadoop fs -copyFromLocal ~/it_tools/static_data/variant.csv

###################
# PySpark scripts #
###################
python ~/it_tools/pyspark_scripts/GetWeatherData.py
hadoop fs -moveFromLocal ~/it_tools/daily_data/weather.json
#################
# Hives scripts #
#################

## Imports data in csv into raw_raw tables
hive -f  ~/it_tools/hive_scripts/raw_db.hql

## Import variant in global (should be done only once)
hive -f ~/it_tools/hive_scripts/global_db.hql

## Updates mirror_db
## Overwrites PRODUCTS table
## Updates PURCHASES table
hive -f ~/it_tools/hive_scripts/mirror_db.hql

## Updates core_db (contains aggregated data)
hive -f ~/it_tools/hive_scripts/core_db.hql

hive -f ~/it_tools/hive_scripts/weather.hql

# cleans the directory 
rm ~/it_tools/daily_data/*.*
