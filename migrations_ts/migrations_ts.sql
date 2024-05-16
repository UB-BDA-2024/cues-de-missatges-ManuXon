-- #TODO: Create new TS hypertable
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS sensor_data ( id INT,  last_seen TIMESTAMPTZ NOT NULL, temperature float , humidity float , velocity float , battery_level float NOT NULL, PRIMARY KEY (id,last_seen) );

SELECT create_hypertable('sensor_data','last_seen');