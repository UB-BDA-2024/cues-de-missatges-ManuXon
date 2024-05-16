from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime
import json
from shared.mongodb_client import MongoDBClient
from shared import redis_client
from shared.sensors import models, schemas
from shared import timescale
from shared.elasticsearch_client import ElasticsearchClient
from shared.cassandra_client import CassandraClient 
from datetime import datetime
from typing import List, Optional


class DataCommand():
    def __init__(self, from_time, to_time, bucket):
        if not from_time or not to_time:
            raise ValueError("from_time and to_time must be provided")
        if not bucket:
            bucket = 'day'
        self.from_time = from_time
        self.to_time = to_time
        self.bucket = bucket


def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    return db_sensor
def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, sensor: schemas.SensorCreate, mongodb: MongoDBClient, es: ElasticsearchClient) -> models.Sensor:
    db_sensor = models.Sensor(name=sensor.name, latitude=sensor.latitude, longitude=sensor.longitude, 
                              type=sensor.type, mac_address=sensor.mac_address, manufacturer=sensor.manufacturer,
                               model=sensor.model, serie_number=sensor.serie_number, firmware_version=sensor.firmware_version,
                                description=sensor.description)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)

    db_sensor_data = sensor.dict()
    mongodb.insert(db_sensor_data)  # Insert the sensor data in the mongodb_collection('sensors')

    es_doc = {
        'id': db_sensor.id,
        'name': sensor.name,
        'latitude': sensor.latitude,
        'longitude': sensor.longitude,
        'type': sensor.type,
        'mac_address': sensor.mac_address,
        'manufacturer': sensor.manufacturer,
        'model': sensor.model,
        'serie_number': sensor.serie_number,
        'firmware_version': sensor.firmware_version,
        'description': sensor.description,
    }
    es.index_document('sensors',es_doc)
    return db_sensor

def record_data(redis: redis_client, db_sensor: models.Sensor, data: schemas.SensorData, timescale: timescale, cassandra: CassandraClient):
    # Store the sensor data in Redis
    json_data = json.dumps(dict(data))  # Serialize the dictionary to JSON (convert SensorData to JSON)
    timescale.insert_sensor_data(db_sensor.id,json_data)
    redis.set(db_sensor.id, json_data)
    cassandra.execute(f"""        
        INSERT INTO count_by_type (sensor_id, sensor_type, time)
        VALUES ({db_sensor.id}, '{db_sensor.type}', '{data.last_seen}')
    """)
    if data.temperature is not None:
        cassandra.execute(f"""
            INSERT INTO temperature_values (sensor_id, timestamp, temperature)
            VALUES ({db_sensor.id},'{data.last_seen}', {data.temperature})
        """)

    if (data.battery_level < 0.2):
        cassandra.execute(f"""        
            INSERT INTO low_battery_sensors (sensor_id, battery_level, time)
            VALUES ({db_sensor.id}, {data.battery_level}, '{data.last_seen}')
        """)
    # Return the recorded data 
    return data

def get_data(redis: redis_client, db_sensor: models.Sensor, timescale: timescale, _from: str, to: str, bucket: str):
   # Convertir las fechas de string a objetos datetime
    from_datetime = datetime.fromisoformat(_from)
    to_datetime = datetime.fromisoformat(to)

    # Validar que la fecha de inicio sea anterior a la fecha de fin
    if from_datetime >= to_datetime:
        raise ValueError("La fecha de inicio debe ser anterior a la fecha de fin")

    # Definir el nombre de la materialized view basado en el intervalo de tiempo
    if bucket == "hour":
        materialized_view = "sensor_data_hour"
    elif bucket == "day":
        materialized_view = "sensor_data_day"
    elif bucket == "week":
        materialized_view = "sensor_data_week"
    elif bucket == "month":
        materialized_view = "sensor_data_month"
    elif bucket == "year":
        materialized_view = "sensor_data_year"
    else:
        raise ValueError("Valor no válido para el parámetro 'bucket'")


    now = datetime.now().isoformat(timespec='milliseconds')
    timescale.enable_autocommit(True)
    query_refresh = f"CALL refresh_continuous_aggregate('{materialized_view}', '2020-01-01T00:00:00.000Z', '{now}Z');"
    timescale.execute(query_refresh)
    # Consultar los datos en la materialized view en TimescaleDB
    query = f"""
        SELECT *
        FROM {materialized_view}
        WHERE id = %s AND {bucket} BETWEEN %s AND %s; """
    try:
        # Ejecutar la consulta en la base de datos
        rows = timescale.fetch_all(query, (db_sensor.id,from_datetime,to_datetime))
        timescale.enable_autocommit(False)
        # Convertir los resultados a una lista de diccionarios
        data = []
        for row in rows:
            data.append({
                "id": row[0],
                "time_bucket": row[1],
                "temperature": row[2],
                "humidity": row[3],
                "velocity": row[4],
                "battery_level": row[5]
            })
        if not data:
            return []
        return data    
    except Exception as e:
        timescale.enable_autocommit(False)
        raise HTTPException(status_code=500, detail=f"Failed to fetch continous_aggregated data: {str(e)}")

# We delete the sensor from PostgreSQL, Redis and MongoDB
def delete_sensor(db: Session, sensor_id: int, mongodb: MongoDBClient, redis: redis_client):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    mongodb.delete(db_sensor.name)
    redis.delete(sensor_id)
    return db_sensor

# We use the mongdb querys to do this method
def get_sensors_near(mongodb: MongoDBClient, latitude: float, longitude: float, radius: float, redis: redis_client, db: Session):
    near = []
    query = {"latitude": {"$gte": latitude - radius, "$lte": latitude + radius},
     "longitude": {"$gte": longitude - radius, "$lte": longitude + radius}}

    sensors = mongodb.collection.find(query) # Do a query for the sensors in a given radius.
    for sensor in sensors:  # Traverse for every sensor in the doc.
        db_sensor = get_sensor_by_name(db,sensor['name'])
        db_sensor_data = get_data(redis, db_sensor) 

        near.append(db_sensor_data)
    return near

def search_sensors(db: Session , mongodb: MongoDBClient, es: ElasticsearchClient, query: str, size: int, search_type: str):  
    # Parse the query string as a JSON object
    query_dict = json.loads(query)

    # Extract field and value from the parsed query
    field = list(query_dict.keys())[0]
    value = query_dict[field]
    print(field,value)
    # Initialize the query DSL based on the search type
    if search_type == 'match':
        es_query = {'query': {'match': {field: {'query': value}}}}
    elif search_type == "similar":
        # Use the fuzzy query for similar search
        es_query = {'query': {'match': {field: {'query': value, 'fuzziness': 'AUTO'}}}}
        print(es_query)
    elif search_type == "prefix":
        # Use the prefix query for prefix search
        es_query = {'query': {'prefix': {field: {'value': value}}}}
    else:
        raise HTTPException(status_code=400, detail="Invalid search type. Must be 'match', 'similar', or 'prefix'.")

    # Include the size parameter in the Elasticsearch query
    es_query["size"] = size

    # Perform the search using Elasticsearch
    es_response = es.search('sensors',es_query)
    print(es_response)
    # Extract information from the Elasticsearch response
    hits = es_response.get("hits", {}).get("hits", [])
    sensors = [hit["_source"] for hit in hits]

    return sensors 

def get_temperature_values(db: Session, cassandra: CassandraClient):
    try:
        sensors = []
         # Define the CQL query to calculate max, min, and average temperature values
        query = """
        SELECT
            sensor_id,
            MAX(temperature) AS max_temperature,
            MIN(temperature) AS min_temperature,
            AVG(temperature) AS average_temperature
        FROM
            sensor.temperature_values
        GROUP BY
            sensor_id
        """
        # Execute the CQL query
        result = cassandra.execute(query)
        
        for row in result:
            db_sensor = get_sensor(db=db, sensor_id=row[0])
            sensors.append({
                "id": row[0],
                "name": db_sensor.name,
                "latitude": db_sensor.latitude,
                "longitude": db_sensor.longitude,
                "type": db_sensor.type,
                "mac_address": db_sensor.mac_address,
                "manufacturer": db_sensor.manufacturer,
                "model": db_sensor.model,
                "serie_number": db_sensor.serie_number,
                "firmware_version": db_sensor.firmware_version,
                "description": db_sensor.description,
                "values": [
                    {
                        "max_temperature": row[1], 
                        "min_temperature": row[2], 
                        "average_temperature": row[3]
                    }
                ]
                })
        return {"sensors": sensors}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch temperature values: {str(e)}")
    
def get_sensors_quantity(db=Session, cassandra=CassandraClient):
    try:
        sensors = []
        # Define the CQL query to get sensor quantities by type
        query = """
        SELECT
            sensor_type, COUNT(sensor_type) AS quantity
        FROM
            sensor.count_by_type
        GROUP BY
            sensor_type
        """

        # Execute the CQL query
        result = cassandra.execute(query)

        for row in result:
            sensors.append({
                "type" : row[0],
                "quantity" : row[1]
            })

        return {"sensors": sensors}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch sensor quantities: {str(e)}")
    

def get_low_battery_sensors(db=Session, cassandra=CassandraClient):
    try:
        # Define the CQL query to get sensors with low battery level
        query = """
            SELECT
            sensor_id, battery_level
        FROM
            sensor.low_battery_sensors
        """

        # Execute the CQL query
        result = cassandra.execute(query)

        # Extract low battery sensors from the result
        low_battery_sensors = result.all()

        # Initialize a list to store the response data
        response_data = []

        # Iterate over the low battery sensors and fetch sensor details from the database
        for sensor_data in low_battery_sensors:
            sensor_id = sensor_data.sensor_id
            battery_level = sensor_data.battery_level
            
            # Fetch sensor details from the database using sensor_id
            sensor_details = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
            if sensor_details:
                sensor_dict = {
                    "id": sensor_details.id,
                    "name": sensor_details.name,
                    "latitude": sensor_details.latitude,
                    "longitude": sensor_details.longitude,
                    "type": sensor_details.type,
                    "mac_address": sensor_details.mac_address,
                    "manufacturer": sensor_details.manufacturer,
                    "model": sensor_details.model,
                    "serie_number": sensor_details.serie_number,
                    "firmware_version": sensor_details.firmware_version,
                    "description": sensor_details.description,
                    "battery_level": battery_level
                }
                response_data.append(sensor_dict)

        # Construct the response JSON
        response = {"sensors": response_data}
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch low battery sensors: {str(e)}")