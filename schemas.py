from pydantic import BaseModel
from typing import Optional

class PropertySchema(BaseModel):
    id: str
    days_on_market: Optional[int]
    margin: Optional[float]
    type_local: Optional[str]
    address: Optional[str]
    latitude: float
    longitude: float

    class Config:
        from_attributes = True

class ClusterSchema(BaseModel):
    latitude: float
    longitude: float
    count: int
    id: Optional[str] = None
    margin: Optional[float] = None
    type_local: Optional[str] = None
    address: Optional[str] = None
