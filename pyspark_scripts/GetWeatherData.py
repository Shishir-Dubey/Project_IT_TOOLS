

import requests
import datetime
import pandas as pd
import calendar as cal

# subscription key to connect th forecast.io
key='03e852635b1ecacad9616ddd5d1a7050'

# ask forecast.io to send figures in decent units: meters and celsius
# in addition: drop informations we don't need: minutly, hourly and currently data
options='?units=si&exclude=currently,minutely,hourly'

# returns weather data for given place and day 
def GetForecast(lat,lon,year,month,day):
    dt = str(cal.timegm(datetime.datetime(year, month, day, 0, 0, 0, 0).timetuple()))
    url='https://api.forecast.io/forecast/'+key+'/'+str(lat)+','+str(lon) + ',' + dt + options
    return requests.get(url).json()['daily']['data'][0]

# Checks that a key belongs to a dictionary. If so, adds it a dataaframe
def InsertValueIfExists(df, index, json, key):
    if key in json:
        return (True, df.set_value(index,key, json[key]))
    else:
        return (False, 'N/A')

# loops over all prefectures' coordinates get their daily weather data
def GetForecasts(year,month,day):
    # Loads into a spark DataFrame the json file that contains prefecture and their cordinates 
    temp = sqlContext.read.json('loc_pref_filled.json')
    # turns a spark DataFrame into an pandas'
    data = temp.toPandas()
    for index in data.index:
        lat = data.get_value(index, 'latitude')
        lon = data.get_value(index, 'longitude')
        resp = GetForecast(lat,lon,year,month,day)
        keys = ['apparentTemperatureMax','apparentTemperatureMin','cloudCover','humidity','precipIntensity','precipProbability','precipType','pressure','summary','temperatureMax','temperatureMin','windSpeed']
        for key in keys:
            iv = InsertValueIfExists(data, index, resp, key)
            if not iv[0]:
                print("No {0:s} on {1:d}-{2:02d}-{3:02d} for {4:s}".format(key, year, month,day,data.get_value(index, 'pref')))
    # drops the columns we don't need: prefecture's name, longitude and latitude
    data.drop(data.columns[[0, 1, 3]], axis=1, inplace=True)
    return data

now = datetime.datetime.now()

data = GetForecasts(now.year,now.month,now.day)

spark_df_data = sqlContext.createDataFrame(data) #.write.json(blob + 'weather_{0:d}_{1:02d}_{2:02d}.json'.format(2016,1,7))

%%sql -q
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
    windSpeed double)

from pyspark.sql import DataFrameWriter

dfw = DataFrameWriter(spark_df_data)
dfw.insertInto('weatherdatatable', overwrite=False)


