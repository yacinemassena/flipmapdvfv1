from sqlalchemy import Column, String, Float, Integer, Index
from database import Base

class Property(Base):
    __tablename__ = "properties"

    id = Column(String, primary_key=True, index=True) # property_id
    days_on_market = Column(Integer)
    margin = Column(Float)
    type_local = Column(String)
    address = Column(String)
    latitude = Column(Float, index=True)
    longitude = Column(Float, index=True)

Index('idx_lat_lon', Property.latitude, Property.longitude)
