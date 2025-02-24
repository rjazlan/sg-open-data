from datetime import datetime
from typing import List, Optional, Dict, Union
from pydantic import BaseModel, Field, ConfigDict


# Common Base Models
class Location(BaseModel):
    latitude: float
    longitude: float


class LabelLocation(BaseModel):
    latitude: float
    longitude: float


class Station(BaseModel):
    id: str
    device_id: str = Field(alias="deviceId")
    name: str
    location: LabelLocation


class BaseResponse(BaseModel):
    code: int
    error_msg: Optional[str] = Field(default=None, alias="errorMsg")


class TimestampedReading(BaseModel):
    timestamp: str
    data: List[Dict]


class TemperatureRange(BaseModel):
    low: float
    high: float
    unit: str


class WindSpeed(BaseModel):
    low: float
    high: float


class Wind(BaseModel):
    speed: WindSpeed
    direction: str


class ValidPeriod(BaseModel):
    start: str
    end: str
    text: str


class WeatherForecast(BaseModel):
    code: Optional[str] = None
    text: str


# Specific Weather Data Models
class WeatherReadingData(BaseModel):
    stations: List[Station]
    readings: List[TimestampedReading]
    reading_type: str = Field(alias="readingType")
    reading_unit: str = Field(alias="readingUnit")
    pagination_token: Optional[str] = Field(default=None, alias="paginationToken")


class AreaMetadata(BaseModel):
    name: str
    location: Location = Field(alias="label_location")


class TwoHourForecastData(BaseModel):
    area_metadata: List[AreaMetadata] = Field(alias="area_metadata")
    items: List[Dict]
    pagination_token: Optional[str] = Field(default=None, alias="paginationToken")


class TwentyFourHourOutlookGeneral(BaseModel):
    temperature: TemperatureRange
    relative_humidity: TemperatureRange = Field(alias="relativeHumidity")
    forecast: WeatherForecast
    valid_period: ValidPeriod = Field(alias="validPeriod")
    wind: Wind


class Regions(BaseModel):
    west: WeatherForecast
    east: WeatherForecast
    central: WeatherForecast
    south: WeatherForecast
    north: WeatherForecast


class TwentyFourHourOutlookPeriod(BaseModel):
    time_period: ValidPeriod = Field(alias="timePeriod")
    regions: Regions


class TwentyFourHourOutlookRecord(BaseModel):
    timestamp: str
    date: str
    updated_timestamp: str = Field(alias="updatedTimestamp")
    general: TwentyFourHourOutlookGeneral
    periods: List[TwentyFourHourOutlookPeriod]


class TwentyFourHourOutlookData(BaseModel):
    records: List[TwentyFourHourOutlookRecord]
    pagination_token: Optional[str] = Field(default=None, alias="paginationToken")


class FourDayOutlookForecast(BaseModel):
    timestamp: str
    temperature: TemperatureRange
    relative_humidity: TemperatureRange = Field(alias="relativeHumidity")
    wind: Wind
    forecast: WeatherForecast
    day: str


class FourDayOutlookData(BaseModel):
    records: List[Dict[str, Union[str, datetime, List[FourDayOutlookForecast]]]]
    pagination_token: Optional[str] = Field(default=None, alias="paginationToken")


class LightningData(BaseModel):
    records: List[Dict]
    pagination_token: Optional[str] = Field(default=None, alias="paginationToken")


class RegionMetadata(BaseModel):
    name: str
    location: Location = Field(alias="labelLocation")


class PollutionRegions(BaseModel):
    west: float
    east: float
    central: float
    south: float
    north: float


class PM25Readings(BaseModel):
    pm25_one_hourly: PollutionRegions


class PM25Item(BaseModel):
    date: str
    updated_timestamp: str = Field(alias="updatedTimestamp")
    timestamp: str
    readings: PM25Readings


class PM25Data(BaseModel):
    region_metadata: List[RegionMetadata] = Field(alias="regionMetadata")
    items: List[PM25Item]
    pagination_token: Optional[str] = Field(default=None, alias="paginationToken")

    # psi_three_hourly: PollutionRegions


class PSIReadings(BaseModel):
    co_sub_index: PollutionRegions
    so2_twenty_four_hourly: PollutionRegions
    so2_sub_index: PollutionRegions
    co_eight_hour_max: PollutionRegions
    no2_one_hour_max: PollutionRegions
    pm10_sub_index: PollutionRegions
    pm25_sub_index: PollutionRegions
    o3_eight_hour_max: PollutionRegions
    psi_twenty_four_hourly: PollutionRegions
    o3_sub_index: PollutionRegions
    pm25_twenty_four_hourly: PollutionRegions
    pm10_twenty_four_hourly: PollutionRegions


class PSIItem(BaseModel):
    date: str
    updated_timestamp: str = Field(alias="updatedTimestamp")
    timestamp: str
    readings: PSIReadings


class PSIData(BaseModel):
    region_metadata: List[RegionMetadata] = Field(alias="regionMetadata")
    items: List[PSIItem]
    pagination_token: Optional[str] = Field(default=None, alias="paginationToken")


class UVIndexData(BaseModel):
    records: List[Dict]
    pagination_token: Optional[str] = Field(default=None, alias="paginationToken")


class WBGTData(BaseModel):
    records: List[Dict]
    pagination_token: Optional[str] = Field(default=None, alias="paginationToken")


# Response Models for each API endpoint
class WeatherResponse(BaseResponse):
    data: WeatherReadingData


class TwoHourForecastResponse(BaseResponse):
    data: TwoHourForecastData


class TwentyFourHourOutlookResponse(BaseResponse):
    data: TwentyFourHourOutlookData


class FourDayOutlookResponse(BaseResponse):
    data: FourDayOutlookData


class LightningResponse(BaseResponse):
    data: LightningData


class PM25Response(BaseResponse):
    data: PM25Data


class PSIResponse(BaseResponse):
    data: PSIData


class UVIndexResponse(BaseResponse):
    data: UVIndexData


class WBGTResponse(BaseResponse):
    data: WBGTData


# Example usage:
"""
# Temperature reading
response = WeatherResponse(
    code=0,
    error_msg=None,
    data={
        "stations": [...],
        "readings": [...],
        "reading_type": "DBT 1M F",
        "reading_unit": "deg C"
    }
)

# Two-hour forecast
forecast = TwoHourForecastResponse(
    code=0,
    error_msg=None,
    data={
        "area_metadata": [...],
        "items": [...]
    }
)
"""
