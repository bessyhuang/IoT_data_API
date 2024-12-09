from pydantic import BaseModel, Field, validator
from fastapi import HTTPException
from typing import Optional
from dateutil.parser import parse, ParserError


def parse_and_format_date(field_value: str) -> str:
    """Helper function to parse and format date."""
    try:
        parsed_date = parse(field_value)
        return parsed_date.strftime("%Y-%m-%d")
    except ParserError as e:
        raise HTTPException(
            status_code=400,
            detail="【Invalid date format】 Expected format: YYYY-MM-DD"
        ) from e


class Item(BaseModel):
    datetime_start: str = Field(..., title='起始日期')  # Required field
    datetime_end: str = Field(..., title='終止日期')
    st_uuid: str = Field(..., title='監測站 UUID')
    pq_uuid: str = Field(..., title='物理量 UUID')

    @validator('datetime_start', 'datetime_end')
    def validate_datetime(cls, value):
        return parse_and_format_date(value)

class Metadata(BaseModel):
    st_uuid: str = Field(..., title='監測站 UUID')
    st_name: str = Field(..., title='監測站名稱 / 抽水機編號')
    pq_uuid: str = Field(..., title='物理量 UUID')
    location: Optional[str] = Field(None, title='所在地點')  # Optional field, can be None
    institution: Optional[str] = Field(None, title='所屬單位')
    datetime_start: str = Field(..., title='抽水區間_起始日期')
    datetime_end: str = Field(..., title='抽水區間_終止日期')

    @validator('datetime_start', 'datetime_end')
    def validate_datetime(cls, value):
        return parse_and_format_date(value)
