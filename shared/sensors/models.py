import datetime
from sqlalchemy import Column, DateTime, Integer, String, Float
from shared.database import Base

class Sensor(Base):
    __tablename__ = "sensors"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
#  joined_at = Column(DateTime, default=datetime.datetime.utcnow)
    type = Column(String, default="Dummy")
    mac_address = Column(String,unique=True, index=True)
    latitude = Column(Float)
    longitude = Column(Float)
    mac_address = Column(String)
    manufacturer = Column(String)
    model = Column(String)
    serie_number = Column(String)
    firmware_version = Column(String)
    description = Column(String)