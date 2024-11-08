import sqlite3
import logging
import os
import time
import configparser

# Initialize logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

# Read configuration from config file
def read_config():
    config = configparser.ConfigParser()
    config.read('config.ini')
    folder_path = config.get('settings', 'folder_path')
    database_path = config.get('settings', 'database_path')
    return folder_path, database_path

# Connect to SQLite database
def connect_db(db_name):
    return sqlite3.connect(db_name)

# Insert or get the station_id from station_codes table based on the filename
def get_station_id(cursor, file_name):
    cursor.execute('''
    INSERT OR IGNORE INTO weather_station (stationCode) 
    VALUES (?)
    ''', (file_name,))
    cursor.connection.commit()

    # Get the station_id for the file
    cursor.execute('''
    SELECT stationId FROM weather_station WHERE stationCode = ?
    ''', (file_name,))
    stationId = cursor.fetchone()[0]
    
    return stationId

# Ingest data from a single file into the database, checking for duplicates
def ingest_data_from_file(cursor, file_path, stationId):
    start_time = time.time()
    logger.info(f"Starting ingestion from file: {file_path}")

    records_ingested = 0

    with open(file_path, 'r') as file:
        lines = file.readlines()

        # Process each data line
        for line in lines:
            data = line.strip().split('\t')

            if len(data) != 4:
                logger.warning(f"Skipping malformed line: {line}")
                continue

            date, maxTemperature, minTemperature, precipitation = data
            maxTemperature = None if maxTemperature == -9999 else maxTemperature
            minTemperature = None if minTemperature == -9999 else minTemperature
            precipitation = None if precipitation == -9999 else precipitation

            try:
                cursor.execute('''
                INSERT OR IGNORE INTO weather_data (stationId, date, maxTemperature, minTemperature, precipitation)
                VALUES (?, ?, ?, ?, ?)
                ''', (stationId, date, maxTemperature, minTemperature, precipitation))

                records_ingested += cursor.rowcount  # Count rows inserted
            except Exception as e:
                logger.error(f"Error inserting data: {e}")

    cursor.connection.commit()

    end_time = time.time()
    duration = end_time - start_time
    logger.info(f"Data ingestion completed from file {file_path} in {duration:.2f} seconds.")
    logger.info(f"{records_ingested} records ingested from {file_path}.")

    return records_ingested
# Calculate the yearly statistics (average max temp, average min temp, total precipitation)
def analyse_yearly_weather(cursor, stationId):
    cursor.execute('''
    SELECT substr(date, 1, 4) AS year, AVG(maxTemperature)/ 10.0 AS avgmaxTemperature, AVG(mintemperature)/ 10.0 AS avgMinTemperature, SUM(precipitation)/ 100.0 AS totalPrecipitation
    FROM weather_data
    WHERE stationId = ?
    GROUP BY year
    ''', (stationId,))

    statistics = cursor.fetchall()
    
    for stat in statistics:
        year, avg_max_temp, avg_min_temp, total_precipitation = stat
        
        # Insert the calculated statistics into the station_yearly_statistics table
        cursor.execute('''
        INSERT OR REPLACE INTO weather_analysis (stationId, year, avgMaxTemperature, avgMinTemperature, totalPrecipitation)
        VALUES (?, ?, ?, ?, ?)
        ''', (stationId, year, avg_max_temp, avg_min_temp, total_precipitation))

    cursor.connection.commit()

# Process all files in the folder
def process_files_in_folder(cursor, folder_path):
    total_records_ingested = 0
    for filename in os.listdir(folder_path):
        if filename.endswith('.txt'):  # Only process .txt files
            file_path = os.path.join(folder_path, filename)
            stationId = get_station_id(cursor, filename)
            records_ingested = ingest_data_from_file(cursor, file_path, stationId)
            analyse_yearly_weather(cursor, stationId)
            total_records_ingested += records_ingested

    return total_records_ingested

# Main function
def main():
    folder_path, database_path = read_config()
    print(folder_path, database_path)
    # Connect to the database
    conn = connect_db(database_path)
    cursor = conn.cursor()

    # Start the ingestion process
    start_time = time.time()
    logger.info("Starting data ingestion from all files.")

    # Process all files in the specified folder
    total_records_ingested = process_files_in_folder(cursor, folder_path)

    end_time = time.time()
    duration = end_time - start_time

    logger.info(f"Data ingestion completed in {duration:.2f} seconds.")
    logger.info(f"Total records ingested: {total_records_ingested}.")

    # Close the connection
    conn.close()

if __name__ == "__main__":
    main()
