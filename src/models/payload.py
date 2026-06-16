from datetime import datetime, timezone
from typing import Optional
from pydantic import BaseModel, Field

class SensorPayload(BaseModel):
    """
    Data contract for MQTT telemetry payload.
    Ensures strict typing and automatic ISO 8601 UTC timestamps.
    """
    device_id: str = Field(
        ..., 
        description="Unique identifier for the edge gateway/device"
    )
    sensor_type: str = Field(
        ..., 
        description="Type of sensor: TEMPERATURE, HUMIDITY, SOIL_MOISTURE, LUMINOSITY"
    )
    reading_value: float = Field(
        ..., 
        description="Raw numerical value from the sensor reading"
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Exact UTC timestamp of the reading"
    )
    metadata_hash: Optional[str] = Field(
        default=None, 
        description="Optional hash for future integrity validation"
    )