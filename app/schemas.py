from pydantic import BaseModel, Field, validator
from fastapi import HTTPException
from typing import Optional, List
from dateutil.parser import parse, ParserError
from enum import Enum


def parse_and_format_date(field_value: str) -> str:
    """Helper function to parse and format date."""
    if 'T' in field_value:
        return field_value
    else:
        try:
            parsed_date = parse(field_value)
            return parsed_date.strftime("%Y-%m-%d")
        except ParserError as e:
            print(e)
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


class Role(str, Enum):
    admin = "admin"
    staff = "staff"
    guest = "guest"


class Function(str, Enum):
    project = "project"
    device_list = "device_list"
    replacement = "replacement"
    map = "map"


class User(BaseModel):
    username: str
    hashed_password: str
    full_name: str
    email: str
    gender: Optional[str] = None
    is_active: bool
    roles: List[Role]
    functions: List[Function]


class UserInDB(User):
    hashed_password: str


class Token(BaseModel):
    access_token: str
    token_type: str
