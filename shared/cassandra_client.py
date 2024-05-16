from cassandra.cluster import Cluster

class CassandraClient:
    def __init__(self, hosts):
        self.cluster = Cluster(hosts,protocol_version=4)
        self.session = self.cluster.connect()
        self.create_keyspace()


    def create_keyspace(self):
        query = """
        CREATE KEYSPACE IF NOT EXISTS sensor WITH replication = {
            'class': 'SimpleStrategy',
            'replication_factor': '1'
        };
        """
        self.session.execute(query)
        self.session.set_keyspace("sensor")

    def get_session(self):
        return self.session

    def close(self):
        self.cluster.shutdown()

    def execute(self, query):
        return self.get_session().execute(query)

    def create_temperature_values_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS sensor.temperature_values (
           sensor_id INT,
           timestamp TIMESTAMP,
           temperature FLOAT,
           PRIMARY KEY (sensor_id, timestamp)
        );
        """
        self.session.execute(query)

    def create_quantity_by_type_table(self):
        query = """
         CREATE TABLE IF NOT EXISTS sensor.count_by_type (
                sensor_id INT,
                sensor_type TEXT,
                time TIMESTAMP,
                PRIMARY KEY (sensor_type, sensor_id)
        );
        """
        self.session.execute(query)

    def create_low_battery_sensors_table(self):
        query = """
       CREATE TABLE IF NOT EXISTS sensor.low_battery_sensors (
                sensor_id INT,
                battery_level DECIMAL,
                time TIMESTAMP,
                PRIMARY KEY (sensor_id, time)
        );
        """
        self.session.execute(query)

    def insert_temperature_values(self, sensor_id, timestamp, temperature):
        query = """
            INSERT INTO temperature_values (sensor_id, timestamp, temperature)
            VALUES (%s, %s, %s)
        """
        prepared_query = self.session.prepare(query)
        bound_query = prepared_query.bind((sensor_id, timestamp, temperature))
        self.session.execute(bound_query)

    def insert_quantity_by_type(self, sensor_type, sensor_id):
        query = """
            INSERT INTO quantity_by_type (sensor_type, sensor_id)
            VALUES (%s, %s)
        """
        prepared_query = self.session.prepare(query)
        bound_query = prepared_query.bind((sensor_type, sensor_id))
        self.session.execute(bound_query)

    def insert_low_battery_sensor(self, sensor_id, battery_level):
        query = """
            INSERT INTO low_battery_sensors (battery_level, sensor_id)
            VALUES (%s, %s)
        """
        prepared_query = self.session.prepare(query)
        bound_query = prepared_query.bind((battery_level, sensor_id))
        self.session.execute(bound_query)