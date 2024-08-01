from fastapi import APIRouter, Depends, Body
from fastapi.responses import FileResponse
from typing import Annotated, Union

from app.schemas import Item

import pandas as pd
import requests
import os


router = APIRouter()


def get_history_data(item):
    base_dir = os.getcwd()
    PAYLOAD = {
        "grant_type": "client_credentials",
        "client_id": "KHMhkO9xrDWrabCVMgRFobTRVOXgC7qJ",
        "client_secret": "7I4A76ecNw0hnhSbAUc6FA=="
    }
    response = requests.post("https://iapi.wra.gov.tw/v3/oauth2/token", data=PAYLOAD).json()
    token = response["access_token"]
    headers = {"Accept": "application/json", "Authorization": "Bearer %s" % token}

    # API: get the metadata of station
    s_API = F"Station/Get/{item.st_uuid}"
    s_response = requests.get(F"https://iapi.wra.gov.tw/v3/api/{s_API}", headers=headers)
    s_response.raise_for_status()
    s_response = s_response.json()
    station_name = s_response['Name']

    All_pq_Dict = dict()
    All_pq_Dict[item.pq_uuid] = []

    # API: get the history data of physical quantity (物理量)
    pq_history_API = F"TimeSeriesData/ReadRawData/{item.pq_uuid}/{item.datetime_start}T00.00.00/{item.datetime_end}T23.59.59/true/480"
    pq_history_response = requests.get(F"https://iapi.wra.gov.tw/v3/api/{pq_history_API}", headers=headers)
    pq_history_response.raise_for_status()
    pq_history_response = pq_history_response.json()

    history = pq_history_response["DataPoints"]
    for h in history:
        All_pq_Dict[item.pq_uuid].append(h)
    df = pd.DataFrame.from_dict(All_pq_Dict[item.pq_uuid])
    df['Station'] = station_name

    excel_filename= F'{station_name}_{item.pq_uuid}.csv'
    df.to_csv(excel_filename, encoding='utf-8-sig', index=False)
    return base_dir, excel_filename


@router.post("/download/RFD", response_class=FileResponse)
async def download_data(item: Annotated[Item, Body(
    examples=[{
            "datetime_start": "2024-07-19",
            "datetime_end": "2024-07-27",
            "st_uuid": "11aa951a-c474-4935-9dc2-648178f3f3f4",
            "pq_uuid": "7c46ce40-5f7a-4f62-b187-8d02cf97e49b",
        }],
    )]):
    base_dir, filename = get_history_data(item)
    file_path = os.path.join(base_dir, filename)
    return FileResponse(file_path, media_type='application/octet-stream', filename=filename)


@router.post("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}
