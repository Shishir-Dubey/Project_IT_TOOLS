import requests
import datetime
import calendar as cal
import json

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

# loops over all prefectures' coordinates get their daily weather data
def GetForecasts(year,month,day):
    result = []
    with open('/home/ivan/it_tools/pyspark_scripts/loc_pref_filled.json') as prefectures:
	data=json.load(prefectures)
        for pref in data:
            row = {}
            lat = pref['latitude']
            lon = pref['longitude']
	    dep = pref['num_departement']
            resp = GetForecast(lat,lon,year,month,day)
            keys = ['apparentTemperatureMax','apparentTemperatureMin','cloudCover','humidity','precipIntensity','precipProbability','precipType','pressure','summary','temperatureMax','temperatureMin','windSpeed']
            for key in keys:
                if key in resp:
                    row[key] = resp[key]
                else:
                    print("No {0:s} on {1:d}-{2:02d}-{3:02d} for {4:s}".format(key, year, month,day, dep))
            result.append(row)
    return result

now = datetime.datetime.now()

data = GetForecasts(now.year,now.month,now.day)

with open('/home/ivan/it_tools/daily_data/weather.json', 'w') as outfile:
    json.dump(data, outfile)
