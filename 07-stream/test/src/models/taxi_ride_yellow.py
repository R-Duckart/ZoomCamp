from datetime import datetime
import logging
from pydantic import BaseModel, field_validator

logger = logging.getLogger(__name__)

class TaxiRideYellow(BaseModel):
    pickup_location_id: int
    dropoff_location_id: int
    trip_distance: float
    total_amount: float
    pickup_datetime: int  # milliseconds

    @classmethod
    def from_row(cls, row):
        return cls(
            pickup_location_id=int(row['PULocationID']),
            dropoff_location_id=int(row['DOLocationID']),
            trip_distance=float(row['trip_distance']),
            total_amount=float(row['total_amount']),
            pickup_datetime=int(row['tpep_pickup_datetime'])
        )

    @field_validator('total_amount')
    def validate_amount(cls, v):
        if v < 0:
            logger.warning(f'Negative total_amount detected: ${v:.2f} - keeping value for analysis')
        return v
    
    @field_validator('trip_distance')
    def validate_distance(cls, v):
        if v < 0:
            logger.warning(f'Negative trip_distance detected: {v:.2f} mi - keeping value for analysis')
        elif v > 1000:
            logger.warning(f'Unusually large trip_distance detected: {v:.2f} mi - possible data quality issue')
        return v
    

    @classmethod
    def from_dict(cls, data):
        return cls(**data)
    
    def to_dict(self):
        return self.model_dump()
    
    def get_datetime(self):
        """Convert to datetime when needed"""
        return datetime.fromtimestamp(self.pickup_datetime / 1000)