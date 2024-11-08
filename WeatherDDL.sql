CREATE TABLE IF NOT EXISTS weather_station (
		stationId INTEGER PRIMARY KEY AUTOINCREMENT,
		stationCode TEXT NOT NULL,
		UNIQUE (stationCode)
);

CREATE TABLE IF NOT EXISTS weather_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
	stationId INTEGER NOT NULL,
        date DATE NOT NULL,
        maxTemperature INTEGER,
        minTemperature INTEGER,
        precipitation INTEGER,
        UNIQUE (date, stationID),
        FOREIGN KEY (stationId) REFERENCES weather_station(id)	
    );
	
CREATE TABLE IF NOT EXISTS weather_analysis (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		stationId INTEGER NOT NULL,
		year INTEGER NOT NULL,
		avgMaxTemperature REAL,
		avgMinTemperature REAL,
		totalPrecipitation REAL,
		UNIQUE (stationID, year),
        FOREIGN KEY (stationId) REFERENCES weather_station(id)				
)