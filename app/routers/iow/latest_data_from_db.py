"""Latest Data API (get data from DB)"""

import os
import json
from decouple import Config, RepositoryEnv
from pymongo import MongoClient
from bson import json_util

from fastapi import APIRouter, File, UploadFile
from fastapi.responses import FileResponse

from app.routers.iow.latest_data import write_file


router = APIRouter()

# Load environment variables
base_dir = os.getcwd()
ENV = Config(RepositoryEnv(base_dir + '/.env'))

# Get IoW metadata from DB
dbClient = MongoClient(
    ENV.get('DB_HOST_PORT'),
    username=ENV.get('DB_USER'),
    password=ENV.get('DB_PASSWORD'),
    authSource=ENV.get('DB_AUTH_SOURCE')
)
db = dbClient.iow


def get_Station_metadata_from_DB(st_uuid):
    s_response = list(db.stations.find({"st_uuid": st_uuid}, {"_id": 0}))
    s_response = json.loads(json_util.dumps(s_response, indent=4))
    return s_response


def get_PhysicalQuantity_metadata_from_DB(pq_uuid):
    pq_metadata_response = list(db.stations.find(
        {"pq": {"$elemMatch": {"pq_id": pq_uuid}}}, {"_id": 0}
    ))
    pq_metadata_response = json.loads(json_util.dumps(pq_metadata_response, indent=4))
    return pq_metadata_response


@router.post("/st_metadata/{st_uuid}")
async def lookup_station_metadata(st_uuid: str):
    return get_Station_metadata_from_DB(st_uuid)


@router.post("/pq_metadata/{pq_uuid}")
async def lookup_physical_quantity_metadata(pq_uuid: str):
    return get_PhysicalQuantity_metadata_from_DB(pq_uuid)


@router.post("/st_pq_relation/", response_class=FileResponse)
async def download_station_and_physical_quantity_relation(st_file: UploadFile = File(...)):
    # Write txt file to temp folder
    try:
        temp_folder_path = base_dir + "/temp/"
        if not os.path.exists(temp_folder_path):
            os.makedirs(temp_folder_path)
        with open(temp_folder_path + st_file.filename, 'wb') as f:
            f.write(st_file.file.read())
    except Exception as e:
        return {"message": "There was an error uploading the file"}

    # Read txt file (One line, one station)
    with open(temp_folder_path + st_file.filename, 'r', encoding='utf-8') as f:
        content = f.readlines()

    data = {}
    for st_uuid in content:
        st_uuid = st_uuid.replace("\n", "")
        pq_list = get_Station_metadata_from_DB(st_uuid)
        data[st_uuid] = pq_list

    # Write json file
    f_name = '監測站_物理量_UUID對應.json'
    f_path = write_file(data, f_name)
    return FileResponse(f_path, media_type='application/octet-stream', filename=f_name)
