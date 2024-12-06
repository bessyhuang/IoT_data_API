"""History Data API"""

from decouple import Config, RepositoryEnv
from typing import Annotated
from datetime import datetime, timedelta
from pymongo import MongoClient
import pandas as pd
import requests
import zipfile
import string
import random
import time
import os

from fastapi import APIRouter, Body, File, UploadFile
from fastapi.responses import FileResponse

from app.schemas import Item
from app.routers.iow.latest_data import get_Station_metadata, write_file


router = APIRouter()

# Load environment variables
base_dir = os.getcwd()
ENV = Config(RepositoryEnv(base_dir + '/.env'))
PAYLOAD = {
    "grant_type": ENV.get("API_GRANT_TYPE"),
    "client_id": ENV.get("API_CLIENT_ID"),
    "client_secret": ENV.get("API_CLIENT_SECRET"),
}

# Get IoW metadata from DB
HISTORY_dbClient = MongoClient(ENV.get('HISTORY_DB_HOST_PORT'), username=ENV.get('HISTORY_DB_USER'), password=ENV.get('HISTORY_DB_PASSWORD'), authSource=ENV.get('HISTORY_DB_AUTH_SOURCE'))
HISTORY_db = HISTORY_dbClient.iow


def get_PhysicalQuantity_history_data_within12hr(
    headers, client_id, client_secret, item, st_name, max_retries=5, delay=4
):
    All_pq_Dict = dict()
    All_pq_Dict[item.pq_uuid] = []

    pq_history_API = f'TimeSeriesData/ReadRawData/{item.pq_uuid}/{item.datetime_start}/{item.datetime_end}/true/480'
    for attempt in range(max_retries):
        try:
            pq_history_response = requests.get(f'https://iapi.wra.gov.tw/v3/api/{pq_history_API}', headers=headers)
            pq_history_response.raise_for_status()
            pq_history_response = pq_history_response.json()

            history = pq_history_response["DataPoints"]
            for h in history:
                All_pq_Dict[item.pq_uuid].append(h)
            df = pd.DataFrame.from_dict(All_pq_Dict[item.pq_uuid])
            df['Station'] = st_name
            return df

        except requests.exceptions.RequestException as e:
            print(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:  # Retry if not the last attempt
                print(f"【 No.{attempt + 1} 】 Retrying in {delay} seconds...")
                time.sleep(delay)
                # Get IoW token
                PAYLOAD = {"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret}
                response = requests.post("https://iapi.wra.gov.tw/v3/oauth2/token", data=PAYLOAD).json()
                token = response["access_token"]
                headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
            else:
                raise
    return None


def get_PhysicalQuantity_history_data(
    headers, client_id, client_secret, item, st_name, max_retries=5, delay=4
):
    All_pq_Dict = dict()
    All_pq_Dict[item.pq_uuid] = []

    pq_history_API = f'TimeSeriesData/ReadRawData/{item.pq_uuid}/{item.datetime_start}T00.00.00/{item.datetime_end}T23.59.59/true/480'
    for attempt in range(max_retries):
        try:
            pq_history_response = requests.get(f'https://iapi.wra.gov.tw/v3/api/{pq_history_API}', headers=headers)
            pq_history_response.raise_for_status()
            pq_history_response = pq_history_response.json()

            history = pq_history_response["DataPoints"]
            for h in history:
                All_pq_Dict[item.pq_uuid].append(h)
            df = pd.DataFrame.from_dict(All_pq_Dict[item.pq_uuid])
            df['Station'] = st_name
            return df

        except requests.exceptions.RequestException as e:
            print(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:  # Retry if not the last attempt
                print(f"【 No.{attempt + 1} 】 Retrying in {delay} seconds...")
                time.sleep(delay)
                # Get IoW token
                PAYLOAD = {"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret}
                response = requests.post("https://iapi.wra.gov.tw/v3/oauth2/token", data=PAYLOAD).json()
                token = response["access_token"]
                headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
            else:
                raise
    return None


def compress(file_names):
    # create the zip file first parameter path/name, second mode
    temp_folder_path = base_dir + "/temp/"
    random_string = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
    f_name = f'DATA_{random_string}.zip'
    zip_filepath = os.path.join(temp_folder_path, f_name)
    try:
        for file_name in file_names:
            with zipfile.ZipFile(zip_filepath, mode='a') as zf:
                zf.write(file_name)
    except FileNotFoundError:
        print("An error occurred")
    return zip_filepath, f_name


@router.post("/download/single_station/raw_data", response_class=FileResponse)
async def download_single_station_raw_data(
    item: Annotated[
            Item,
            Body(
                examples=[{
                    "datetime_start": "2024-07-01",
                    "datetime_end": "2024-07-31",
                    "st_uuid": "",
                    "pq_uuid": "",
                }],
            )
        ]
):
    # Get IoW token
    response = requests.post("https://iapi.wra.gov.tw/v3/oauth2/token", data=PAYLOAD).json()
    token = response["access_token"]
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}

    # API
    s_response = get_Station_metadata(item.st_uuid, headers, PAYLOAD["client_id"], PAYLOAD["client_secret"])
    st_name = s_response['Name']

    df = get_PhysicalQuantity_history_data(headers, PAYLOAD["client_id"], PAYLOAD["client_secret"], item, st_name)
    f_name = f'{st_name}_{item.pq_uuid}.csv'
    f_path = write_file(df, f_name)
    return FileResponse(f_path, media_type='application/octet-stream', filename=f_name)


@router.post("/download/multiple_stations/raw_data", response_class=FileResponse)
async def download_multiple_stations_raw_data(
    client_id: str,
    client_secret: str,
    datetime_start: str,
    datetime_end: str,
    st_pq_file: UploadFile = File(...)
):
    # Get IoW token
    PAYLOAD = {"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret}
    response = requests.post("https://iapi.wra.gov.tw/v3/oauth2/token", data=PAYLOAD).json()
    token = response["access_token"]
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}

    # Write txt file to temp folder
    try:
        temp_folder_path = base_dir + "/temp/"
        if not os.path.exists(temp_folder_path):
            os.makedirs(temp_folder_path)
        with open(temp_folder_path + st_pq_file.filename, 'wb') as f:
            f.write(st_pq_file.file.read())
    except:
        return {"message": "There was an error uploading the file"}

    # Read txt file (One line, one station)
    with open(temp_folder_path + st_pq_file.filename, 'r', encoding='utf-8') as f:
        content = f.readlines()

    file_names = []
    for line in content:
        line = line.replace("\n", "").replace("\t", ",")
        line = line.split(",")
        st_uuid = line[0]
        pq_uuid = line[1]

        # API
        s_response = get_Station_metadata(st_uuid, headers, PAYLOAD["client_id"], PAYLOAD["client_secret"])
        st_name = s_response['Name']

        item = Item(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            st_uuid=st_uuid,
            pq_uuid=pq_uuid
        )
        df = get_PhysicalQuantity_history_data(headers, PAYLOAD["client_id"], PAYLOAD["client_secret"], item, st_name)
        f_name = f'{st_name}_{pq_uuid}.csv'
        f_path = write_file(df, f_name)
        file_names.append(f_path)
    zip_filepath, f_name = compress(file_names)
    return FileResponse(zip_filepath, media_type='application/octet-stream', filename=f_name)


@router.post("/report/avail_rate_daily", response_class=FileResponse)
async def 日妥善率歷史報表(
    datetime_start: str,
    datetime_end: str,
):
    format_datetime_start = datetime.strptime(datetime_start, '%Y-%m-%d') + timedelta(hours=0, minutes=0, seconds=0)
    format_datetime_end = datetime.strptime(datetime_end, '%Y-%m-%d') + timedelta(hours=23, minutes=59, seconds=59)
    data = list(HISTORY_db.avail_rate.find(
        {
            "date": {
                "$gte": format_datetime_start,
                "$lte": format_datetime_end
            }
        },
        {
            "_id": 0, "date": 0
        }
    ))
    df = pd.DataFrame.from_dict(data)
    f_name = f'日妥善率歷史報表_{datetime_start}_{datetime_end}.csv'
    f_path = write_file(df, f_name)
    return FileResponse(f_path, media_type="application/octet-stream", filename=f_name)