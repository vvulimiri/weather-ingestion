Absolute path of Raw files has to be added in config.ini.
  folder_path = C:\Users\manasa\WeatherIngestion\RawFiles
Absolute path for database has to be added.
  database_path = WeatherDB.db


weatherDataIngestion.py 
This file contains the code for weather data ingestion from RawFiles and stores into table weather_data.
The data is analysed and averages and total are computed by year for each station and saved into weather_analysis table.
weather_station contains metadata mapping - stationId, stationCode where stationCode represents the fileName and stationId is the incremental ID value.
