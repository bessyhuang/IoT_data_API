from pydantic import BaseModel, HttpUrl

from typing import Sequence


class Item(BaseModel):
    datetime_start: str
    datetime_end: str
    st_uuid: str
    pq_uuid: str

class Pump(BaseModel):
    st_uuid: str
    st_name: str         # 抽水機編號
    pq_uuid: str
    location: str        # 所在地點
    institution: str     # 所屬單位
    datetime_start: str  # 抽水區間 (Start_TimeStamp)
    datetime_end: str    # 抽水區間 (End_TimeStamp)
    # volume: float        # 抽水量(立方公尺)
    # duration: str        # 抽水(分)