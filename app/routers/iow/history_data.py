from fastapi import APIRouter, Body
from fastapi.responses import FileResponse
from typing import Annotated, Union

from app.schemas import Item
from app.routers.iow.latest_data import get_Station_metadata, write_file

from decouple import Config, RepositoryEnv
import pandas as pd
import requests
import os


router = APIRouter()

# Load environment variables
base_dir = os.getcwd()
ENV = Config(RepositoryEnv(base_dir + '/.env'))
PAYLOAD = {
    "grant_type": ENV.get("API_GRANT_TYPE"),
    "client_id": ENV.get("API_CLIENT_ID"),
    "client_secret": ENV.get("API_CLIENT_SECRET"),
}


def get_PhysicalQuantity_history_data(headers, item, st_name):
    All_pq_Dict = dict()
    All_pq_Dict[item.pq_uuid] = []

    pq_history_API = F"TimeSeriesData/ReadRawData/{item.pq_uuid}/{item.datetime_start}T00.00.00/{item.datetime_end}T23.59.59/true/480"
    pq_history_response = requests.get(F"https://iapi.wra.gov.tw/v3/api/{pq_history_API}", headers=headers)
    pq_history_response.raise_for_status()
    pq_history_response = pq_history_response.json()

    history = pq_history_response["DataPoints"]
    for h in history:
        All_pq_Dict[item.pq_uuid].append(h)
    df = pd.DataFrame.from_dict(All_pq_Dict[item.pq_uuid])
    df['Station'] = st_name
    return df


@router.post("/download/raw_data", response_class=FileResponse)
async def download_raw_data(item: Annotated[Item, Body(
    examples=[{
            "datetime_start": "2024-07-01",
            "datetime_end": "2024-07-31",
            "st_uuid": "",
            "pq_uuid": "",
        }],
    )]):

    # Get IoW token
    response = requests.post("https://iapi.wra.gov.tw/v3/oauth2/token", data=PAYLOAD).json()
    token = response["access_token"]
    headers = {"Accept": "application/json", "Authorization": "Bearer %s" % token}

    # API
    s_response = get_Station_metadata(item.st_uuid, headers)
    st_name = s_response['Name']

    df = get_PhysicalQuantity_history_data(headers, item, st_name)
    f_name = F'{st_name}_{item.pq_uuid}.csv'
    f_path = write_file(df, f_name)
    return FileResponse(f_path, media_type='application/octet-stream', filename=f_name)


@router.post("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}
