CREATE MATERIALIZED VIEW IF NOT EXISTS sensor_data_hour
WITH (timescaledb.continuous) AS
SELECT 
  id,
  time_bucket( INTERVAL '1 hour', last_seen) AS hour,
  AVG(velocity) AS avg_velocity,
  AVG(temperature) AS avg_temperature,
  AVG(humidity) AS avg_humidity,
  AVG(battery_level) AS avg_battery
FROM 
  sensor_data
GROUP BY 
  id, hour; 

SELECT add_continuous_aggregate_policy('sensor_data_hour',
  start_offset => NULL,   
  end_offset => INTERVAL '1 h',      
  schedule_interval => INTERVAL '1 minute');  

-- Daily Materialized View
CREATE MATERIALIZED VIEW IF NOT EXISTS sensor_data_day
WITH (timescaledb.continuous) AS
SELECT 
       id, 
       time_bucket('1 day', last_seen) AS day,
       avg(temperature) AS avg_temperature,
       avg(humidity) AS avg_humidity,
       avg(velocity) AS avg_velocity,
       avg(battery_level) AS avg_battery_level
FROM 
 sensor_data
GROUP BY
    id,day;

SELECT add_continuous_aggregate_policy('sensor_data_day',
  start_offset => NULL,   
  end_offset => INTERVAL '1 h',      
  schedule_interval => INTERVAL '1 h');

-- Weekly Materialized View
CREATE MATERIALIZED VIEW IF NOT EXISTS sensor_data_week
WITH (timescaledb.continuous) AS
SELECT
    id,
    time_bucket('1 week', last_seen) AS month,
       avg(temperature) AS avg_temperature,
       avg(humidity) AS avg_humidity,
       avg(velocity) AS avg_velocity,
       avg(battery_level) AS avg_battery_level
FROM sensor_data
GROUP BY 
    id,month;

SELECT add_continuous_aggregate_policy('sensor_data_week',
  start_offset => NULL,   
  end_offset => INTERVAL '1 h',      
  schedule_interval => INTERVAL '1 h');

-- Monthly Materialized View
CREATE MATERIALIZED VIEW IF NOT EXISTS sensor_data_month
WITH (timescaledb.continuous) AS
SELECT 
       id,
       time_bucket('1 month', last_seen) AS month,
       avg(temperature) AS avg_temperature,
       avg(humidity) AS avg_humidity,
       avg(velocity) AS avg_velocity,
       avg(battery_level) AS avg_battery_level
FROM sensor_data
GROUP BY id,month;

SELECT add_continuous_aggregate_policy('sensor_data_month',
  start_offset => NULL,   
  end_offset => INTERVAL '1 h',      
  schedule_interval => INTERVAL '1 h'); 

-- Yearly Materialized View
CREATE MATERIALIZED VIEW IF NOT EXISTS sensor_data_year
WITH (timescaledb.continuous) AS
SELECT 
       id,
       time_bucket('1 year', last_seen) AS year,
       avg(temperature) AS avg_temperature,
       avg(humidity) AS avg_humidity,
       avg(velocity) AS avg_velocity,
       avg(battery_level) AS avg_battery_level
FROM sensor_data
GROUP BY id,year;

SELECT add_continuous_aggregate_policy('sensor_data_year',
  start_offset => NULL,   
  end_offset => INTERVAL '1 h',      
  schedule_interval => INTERVAL '1 h'); 