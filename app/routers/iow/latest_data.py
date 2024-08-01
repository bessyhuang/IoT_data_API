from fastapi import APIRouter, File, UploadFile
from fastapi.responses import FileResponse

import requests
import json
import os


router = APIRouter()
base_dir = os.getcwd()


def get_Station_metadata(st_uuid, headers):
    s_API = F"Station/Get/{st_uuid}"
    s_response = requests.get(F"https://iapi.wra.gov.tw/v3/api/{s_API}", headers=headers)
    s_response.raise_for_status()
    s_response = s_response.json()

    if s_response["JsonProperties"] != None:
        json_object_JsonProperties = json.loads(s_response["JsonProperties"])
        del s_response["JsonProperties"]
        s_response["JsonProperties"] = json_object_JsonProperties
    return s_response

def get_PhysicalQuantity_UUIDs(st_uuid, headers):
    pq_API = F"LatestData/Read/Station/{st_uuid}/480"
    try:
        pq_response = requests.get(F"https://iapi.wra.gov.tw/v3/api/{pq_API}", headers=headers)
    except:
        pq_response = requests.get(F"https://iapi.wra.gov.tw/v3/api/{pq_API}", headers=headers)
    pq_response.raise_for_status()
    pq_response = pq_response.json()

    if pq_response == None:
        return []
    else:
        pqUUIDs_list = []
        for pq in pq_response:
            pqUUIDs_list.append(pq['Id'])
        return pqUUIDs_list

def get_PhysicalQuantity_metadata(pq_uuid, headers):
    pq_metadata_API = F"PhysicalQuantity/Get/{pq_uuid}"
    pq_metadata_response = requests.get(F"https://iapi.wra.gov.tw/v3/api/{pq_metadata_API}", headers=headers)
    pq_metadata_response.raise_for_status()
    pq_metadata_response = pq_metadata_response.json()

    if pq_metadata_response["JsonProerties"] != None:
        json_object_JsonProperties = json.loads(pq_metadata_response["JsonProerties"])
        del pq_metadata_response["JsonProerties"]
        pq_metadata_response["JsonProerties"] = json_object_JsonProperties
    return pq_metadata_response

def write_file(data, filename):
    file_type = filename.split(".")[-1]
    temp_folder_path = base_dir + "/temp/"
    if not os.path.exists(temp_folder_path):
        os.makedirs(temp_folder_path)
    output_file = os.path.join(temp_folder_path, filename)
    if file_type == 'json':
        with open(output_file, 'w') as fp:
            json.dump(data, fp, indent=4)
    elif file_type == 'csv':
        data.to_csv(output_file, encoding='utf-8-sig', index=False)
    return output_file

@router.post("/pq_uuid_list/{st_uuid}")
async def lookup_physical_quantity_list(client_id: str, client_secret: str, st_uuid: str):
    # Get IoW token
    PAYLOAD = {"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret}
    response = requests.post("https://iapi.wra.gov.tw/v3/oauth2/token", data=PAYLOAD).json()
    token = response["access_token"]
    headers = {"Accept": "application/json", "Authorization": "Bearer %s" % token}

    # API
    s_response = get_Station_metadata(st_uuid, headers)
    pq_uuid_list = get_PhysicalQuantity_UUIDs(st_uuid, headers)
    return {"st_metadata": s_response, "pq_list": pq_uuid_list}


@router.post("/st_metadata/{st_uuid}")
async def lookup_station_metadata(client_id: str, client_secret: str, st_uuid: str):
    # Get IoW token
    PAYLOAD = {"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret}
    response = requests.post("https://iapi.wra.gov.tw/v3/oauth2/token", data=PAYLOAD).json()
    token = response["access_token"]
    headers = {"Accept": "application/json", "Authorization": "Bearer %s" % token}

    # API
    s_response = get_Station_metadata(st_uuid, headers)
    pq_uuid_list = get_PhysicalQuantity_UUIDs(st_uuid, headers)
    pq_metadata = []
    if pq_uuid_list != []:
        for pq_uuid in pq_uuid_list:
            data = get_PhysicalQuantity_metadata(pq_uuid, headers)
            pq_metadata.append(data)
        return {"st_metadata": s_response, "pq_list": pq_uuid_list, "pq_metadata": pq_metadata}
    else:
        return {"st_metadata": s_response, "pq_list": pq_uuid_list}


@router.post("/st_pq_relation/", response_class=FileResponse)
async def download_station_and_physical_quantity_relation(client_id: str, client_secret: str, file: UploadFile = File(...)):
    # Get IoW token
    PAYLOAD = {"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret}
    response = requests.post("https://iapi.wra.gov.tw/v3/oauth2/token", data=PAYLOAD).json()
    token = response["access_token"]
    headers = {"Accept": "application/json", "Authorization": "Bearer %s" % token}

    # Write txt file to temp folder
    try:
        temp_folder_path = base_dir + "/temp/"
        if not os.path.exists(temp_folder_path):
            os.makedirs(temp_folder_path)
        with open(temp_folder_path + file.filename, 'wb') as f:
            f.write(file.file.read())
    except Exception:
        return {"message": "There was an error uploading the file"}

    # Read txt file (One line, one station)
    with open(temp_folder_path + file.filename, 'r') as f:
        content = f.readlines()

    data = dict()
    for st_uuid in content:
        st_uuid = st_uuid.replace("\n", "")
        pq_list = get_PhysicalQuantity_UUIDs(st_uuid, headers)
        data[st_uuid] = pq_list

    # Write json file
    f_name = F'監測站_物理量_UUID對應.json'
    f_path = write_file(data, f_name)
    return FileResponse(f_path, media_type='application/octet-stream', filename=f_name)
