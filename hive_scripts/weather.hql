CREATE DATABASE IF NOT EXISTS weather_db; 

use weather_db; 

CREATE TABLE IF NOT EXISTS weatherdatatable (
    num_departement string,
    apparentTemperatureMax string,
    apparentTemperatureMin double,
    cloudCover double,
    humidity double,
    precipIntensity double,
    precipProbability double,
    precipType string,
    pressure double,
    summary string,
    temperatureMax double,
    temperatureMin double,
    windSpeed double); 

LOAD DATA INPATH 'weather.csv' INTO TABLE weatherdatatable;
