require 'bundler/setup'
Bundler.require(:default, :test, :development)
require 'date'

conn = PG.connect(dbname: 'citibike', port: 5433)

conn.exec <<-SQL
  CREATE TABLE if not exists stations (
           id            INTEGER      NOT NULL,
           status        TEXT         NOT NULL,
           total_docks   INTEGER      NOT NULL,
           latitude      FLOAT        NOT NULL,
           longitude     FLOAT        NOT NULL,
           label         TEXT         NOT NULL,
           time          TIMESTAMP    NOT NULL,
  );
SQL
conn.exec <<-SQL
  CREATE TABLE if not exists available_bikes (
           station_id   INTEGER   NOT NULL,
           time         TIMESTAMP NOT NULL,
           count        INTEGER   NOT NULL
  );
SQL


def scrape(conn)
  # http://citibikenyc.com/system-data
  response = HTTParty.get('http://citibikenyc.com/stations/json')

  response['stationBeanList'].each do |station|
    # Save the station
    station_row = [
      station['id'],
      station['statusValue'],
      station['totalDocks'],
      station['latitude'],
      station['longitude'],
      station['stationName'],
      DateTime.parse(station['lastCommunicationTime'])
    ]
    # Insert the station into the stations table, if it's not already there
    query = <<-SQL
      INSERT INTO stations
      (id, status, total_docks, latitude, longitude, label, time)
      VALUES
      ($1, $2, $3, $4, $5, $6, $7)
      ON CONFLICT (id, total_docks)
      DO NOTHING
    SQL
    conn.exec_params query, station_row

    # Only add the available count to available_bikes if the station is in service.
    next unless station['statusValue'] == 'In Service'
    available_bikes_row = [
      station['id'],
      DateTime.parse(station['lastCommunicationTime']),
      station['availableBikes']
    ]
    # Insert the entry into the available_bikes table
    query = <<-SQL
        INSERT INTO available_bikes
        (station_id, time, count)
        VALUES
        ($1, $2, $3)
    SQL
    conn.exec_params query, available_bikes_row
  end
end


# Loop forever and run scrape every 10 minutes
half_hour_in_secs = 60 * 30
ten_mins_in_secs = 60 * 10
sleep_duration = ten_mins_in_secs
loop do
  start = Time.new
  puts "Scraping more data at #{start}"

  scrape conn
  # Subtract the amount of time it took to actually scrape to maintain exactly
  # every half hour
  sleep sleep_duration - (Time.new - start)
end
