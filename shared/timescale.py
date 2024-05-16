import psycopg2
import os


class Timescale:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.environ.get("TS_HOST"),
            port=os.environ.get("TS_PORT"),
            user=os.environ.get("TS_USER"),
            password=os.environ.get("TS_PASSWORD"),
            database=os.environ.get("TS_DBNAME"))
        self.cursor = self.conn.cursor()
        
    def getCursor(self):
            return self.cursor

    def close(self):
        self.cursor.close()
        self.conn.close()
    
    def ping(self):
        return self.conn.ping()
    
    def execute(self, query):
       return self.cursor.execute(query)
    
    def enable_autocommit(self, enable=True):
        self.conn.autocommit = enable

    def fetch_all(self, query, params=None):
        self.cursor.execute(query, params)
        return self.cursor.fetchall()
    
    def delete(self, table):
        self.cursor.execute("DELETE FROM " + table)
        self.conn.commit()

    def insert_sensor_data(self,sensor_id,data):
        try:
            columns = []
            values = []
            placeholders = []

            for key,value in data.items():
                columns.append(key)
                values.append(value)
                placeholders.append("%s")

            # Convert to , separated

            columns_str = ", ".join(columns)
            placeholders_str = ", ".join(placeholders)

            # Construct the SQL INSERT query
            query = f"""
            INSERT INTO sensor_data (id, {columns_str})
            VALUES (%s, {placeholders_str});
            """

            values.insert(0,sensor_id)

            # Execute the query with data
            self.cursor.execute(query, tuple(values))
            self.conn.commit()
        except Exception as e:
            # Handle any exceptions
            print(f"Error inserting sensor data: {e}")

     
         
