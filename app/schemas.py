from pydantic import BaseModel, HttpUrl

from typing import Sequence


class Item(BaseModel):
    datetime_start: str
    datetime_end: str
    st_uuid: str
    pq_uuid: str
