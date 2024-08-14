from pydantic import BaseModel

from typing import Optional


class Item(BaseModel):
    datetime_start: str
    datetime_end: str
    st_uuid: str
    pq_uuid: str

class Metadata(BaseModel):
    st_uuid: str
    st_name: str         # 抽水機編號
    pq_uuid: str
    location: Optional[str] = None       # 所在地點
    institution: Optional[str] = None    # 所屬單位
    datetime_start: str                  # 抽水區間 (Start_TimeStamp)
    datetime_end: str                    # 抽水區間 (End_TimeStamp)