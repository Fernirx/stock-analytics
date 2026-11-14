from datetime import datetime
from pydantic import BaseModel, field_serializer


class TickSchema(BaseModel):
    symbol: str
    price: float
    volume: float
    match_type: str = "U"
    trade_time: datetime

    @field_serializer("trade_time")
    def serialize_datetime(self, dt: datetime, _info):
        return dt.strftime("%Y-%m-%d %H:%M:%S")
