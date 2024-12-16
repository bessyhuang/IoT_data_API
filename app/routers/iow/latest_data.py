"""Latest Data API"""

from collections import defaultdict
import pandas as pd
import requests
import json
import time
import os

from fastapi import APIRouter, File, UploadFile, Query
from fastapi.responses import FileResponse

router = APIRouter()
base_dir = os.getcwd()


def get_Station_metadata(
    st_uuid, headers, client_id, client_secret, max_retries=5, delay=4
):
    s_API = f'Station/Get/{st_uuid}'

    for attempt in range(max_retries):
        try:
            s_response = requests.get(f'https://iapi.wra.gov.tw/v3/api/{s_API}', headers=headers, timeout=5)
            s_response.raise_for_status()
            s_response = s_response.json()

            if s_response.get("JsonProperties") is not None:
                json_object_JsonProperties = json.loads(s_response["JsonProperties"])
                del s_response["JsonProperties"]
                s_response["JsonProperties"] = json_object_JsonProperties
            return s_response

        except requests.exceptions.RequestException as e:
            print(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:  # Retry if not the last attempt
                print(f"【 No.{attempt + 1} 】 Retrying in {delay} seconds...")
                time.sleep(delay)
                # Get IoW token
                PAYLOAD = {"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret}
                response = requests.post("https://iapi.wra.gov.tw/v3/oauth2/token", data=PAYLOAD, timeout=5).json()
                token = response["access_token"]
                headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
                continue
    return None

def get_PhysicalQuantity_UUIDs(
    st_uuid, headers, client_id, client_secret, max_retries=5, delay=4
):
    pq_API = f'LatestData/Read/Station/{st_uuid}/480'

    for attempt in range(max_retries):
        try:
            pq_response = requests.get(f'https://iapi.wra.gov.tw/v3/api/{pq_API}', headers=headers, timeout=5)
            pq_response.raise_for_status()
            pq_response = pq_response.json()

            pq_uuids_list = []
            if pq_response is not None:
                for pq in pq_response:
                    pq_uuids_list.append(pq['Id'])
            return pq_uuids_list

        except requests.exceptions.RequestException as e:
            print(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:  # Retry if not the last attempt
                print(f"【 No.{attempt + 1} 】 Retrying in {delay} seconds...")
                time.sleep(delay)
                # Get IoW token
                PAYLOAD = {"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret}
                response = requests.post("https://iapi.wra.gov.tw/v3/oauth2/token", data=PAYLOAD, timeout=5).json()
                token = response["access_token"]
                headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
            else:
                raise
    return None

def get_PhysicalQuantity_latest_data(
    st_uuid, headers, client_id, client_secret, max_retries=5, delay=4
):
    pq_API = f'LatestData/Read/Station/{st_uuid}/480'

    for attempt in range(max_retries):
        try:
            pq_response = requests.get(f'https://iapi.wra.gov.tw/v3/api/{pq_API}', headers=headers, timeout=5)
            pq_response.raise_for_status()
            pq_response = pq_response.json()

            pq_uuids_dict = defaultdict(dict)
            if pq_response is not None:
                for pq in pq_response:
                    pq_uuids_dict[pq['Id']]["Value"] = pq['Value']
                    pq_uuids_dict[pq['Id']]["TimeStamp"] = pq['TimeStamp']
            return pq_uuids_dict

        except requests.exceptions.RequestException as e:
            print(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:  # Retry if not the last attempt
                print(f"【 No.{attempt + 1} 】 Retrying in {delay} seconds...")
                time.sleep(delay)
                # Get IoW token
                PAYLOAD = {"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret}
                response = requests.post("https://iapi.wra.gov.tw/v3/oauth2/token", data=PAYLOAD, timeout=5).json()
                token = response["access_token"]
                headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
            else:
                raise
    return None

def get_PhysicalQuantity_metadata(
    pq_uuid, headers, client_id, client_secret, max_retries=5, delay=4
):
    pq_metadata_API = f'PhysicalQuantity/Get/{pq_uuid}'

    for attempt in range(max_retries):
        try:
            pq_metadata_response = requests.get(f'https://iapi.wra.gov.tw/v3/api/{pq_metadata_API}', headers=headers, timeout=5)
            pq_metadata_response.raise_for_status()
            pq_metadata_response = pq_metadata_response.json()

            if pq_metadata_response["JsonProerties"] is not None:
                json_object_JsonProperties = json.loads(pq_metadata_response["JsonProerties"])
                del pq_metadata_response["JsonProerties"]
                pq_metadata_response["JsonProerties"] = json_object_JsonProperties
            return pq_metadata_response

        except requests.exceptions.RequestException as e:
            print(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:  # Retry if not the last attempt
                print(f"【 No.{attempt + 1} 】 Retrying in {delay} seconds...")
                time.sleep(delay)
                # Get IoW token
                PAYLOAD = {"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret}
                response = requests.post("https://iapi.wra.gov.tw/v3/oauth2/token", data=PAYLOAD, timeout=5).json()
                token = response["access_token"]
                headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
            else:
                raise
    return None

def write_file(data, filename):
    file_type = filename.split(".")[-1]
    temp_folder_path = base_dir + "/temp/"
    if not os.path.exists(temp_folder_path):
        os.makedirs(temp_folder_path)
    output_file = os.path.join(temp_folder_path, filename)
    if file_type == 'json':
        with open(output_file, 'w', encoding="utf-8") as fp:
            json.dump(data, fp, indent=4, ensure_ascii=False)
    elif file_type == 'csv':
        data.to_csv(output_file, encoding='utf-8-sig', index=False)
    return output_file


@router.post("/pq_uuid_list/{st_uuid}")
async def lookup_physical_quantity_list(
    client_id: str, client_secret: str, st_uuid: str
):
    # Get IoW token
    PAYLOAD = {"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret}
    response = requests.post("https://iapi.wra.gov.tw/v3/oauth2/token", data=PAYLOAD, timeout=5).json()
    token = response["access_token"]
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}

    # API
    s_response = await get_Station_metadata(st_uuid, headers, client_id, client_secret)
    pq_uuid_list = await get_PhysicalQuantity_UUIDs(st_uuid, headers, client_id, client_secret)
    return {"st_metadata": s_response, "pq_list": pq_uuid_list}


@router.post("/st_metadata/{st_uuid}")
async def lookup_station_metadata(
    client_id: str, client_secret: str, st_uuid: str
):
    # Get IoW token
    PAYLOAD = {"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret}
    response = requests.post("https://iapi.wra.gov.tw/v3/oauth2/token", data=PAYLOAD, timeout=5).json()
    token = response["access_token"]
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}

    # API
    s_response = await get_Station_metadata(st_uuid, headers, client_id, client_secret)
    pq_uuid_list = await get_PhysicalQuantity_UUIDs(st_uuid, headers, client_id, client_secret)
    pq_metadata = []
    if pq_uuid_list:
        for pq_uuid in pq_uuid_list:
            data = await get_PhysicalQuantity_metadata(pq_uuid, headers, client_id, client_secret)
            pq_metadata.append(data)
    return {"st_metadata": s_response, "pq_list": pq_uuid_list, "pq_metadata": pq_metadata}


@router.post("/st_pq_relation/", response_class=FileResponse)
async def download_station_and_physical_quantity_relation(client_id: str, client_secret: str, st_file: UploadFile = File(...)):
    # Get IoW token
    PAYLOAD = {"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret}
    response = requests.post("https://iapi.wra.gov.tw/v3/oauth2/token", data=PAYLOAD, timeout=5).json()
    token = response["access_token"]
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}

    # Write txt file to temp folder
    try:
        temp_folder_path = base_dir + "/temp/"
        if not os.path.exists(temp_folder_path):
            os.makedirs(temp_folder_path)
        with open(temp_folder_path + st_file.filename, 'wb') as f:
            f.write(st_file.file.read())
    except:
        return {"message": "There was an error uploading the file"}

    # Read txt file (One line, one station)
    with open(temp_folder_path + st_file.filename, 'r', encoding='utf-8') as f:
        content = f.readlines()

    data = {}
    for st_uuid in content:
        st_uuid = st_uuid.replace("\n", "")
        pq_list = await get_PhysicalQuantity_UUIDs(st_uuid, headers, client_id, client_secret)
        data[st_uuid] = pq_list

    # Write json file
    f_name = '監測站_物理量_UUID對應.json'
    f_path = write_file(data, f_name)
    return FileResponse(f_path, media_type='application/octet-stream', filename=f_name)


@router.post("/get_latest_table")
async def 監測站與物理量UUID對應表(
    client_id: str, client_secret: str, 
    device_type: str = Query('RFD', enum=['RFD', 'MPD', 'MPDCY'])
):
    # Get IoW token
    PAYLOAD = {"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret}
    response = requests.post("https://iapi.wra.gov.tw/v3/oauth2/token", data=PAYLOAD, timeout=5).json()
    token = response["access_token"]
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}

    st_uuids_path = base_dir + "/STATION_UUIDs/" + f'{device_type}_station_ID.txt'
    with open(st_uuids_path, "r", encoding='utf-8') as f:
        st_uuids_list = f.readlines()
        st_uuids_list = [row.replace('\n', '') for row in st_uuids_list]

    # API
    data = defaultdict(dict)
    merge_list = []
    for st_uuid in st_uuids_list:
        s_response = await get_Station_metadata(st_uuid, headers, client_id, client_secret)
        pq_uuid_list = await get_PhysicalQuantity_UUIDs(st_uuid, headers, client_id, client_secret)
        data[st_uuid]["st_uuid"] = st_uuid
        data[st_uuid]["st_name"] = s_response["Name"]

        if pq_uuid_list:
            for pq_uuid in pq_uuid_list:
                pq_metadata_response = await get_PhysicalQuantity_metadata(pq_uuid, headers, client_id, client_secret)
                PQ_desc = pq_metadata_response["Description"]
                PQ_name = pq_metadata_response["Name"]

                if device_type == 'RFD':
                    if PQ_name == "淹水深度":
                        data[st_uuid]["淹水深度"] = pq_uuid
                    if "主機電壓" in PQ_desc:
                        data[st_uuid]["主機電壓"] = pq_uuid
                elif device_type in ('MPD', 'MPDCY'):
                    if "經度" in PQ_desc:
                        data[st_uuid]["經度"] = pq_uuid
                    if "緯度" in PQ_desc:
                        data[st_uuid]["緯度"] = pq_uuid
                    if "運作狀態" in PQ_desc:
                        data[st_uuid]["運作狀態"] = pq_uuid
                    if "GPS主機電壓" in PQ_desc:
                        data[st_uuid]["GPS主機電壓"] = pq_uuid
                    if "電瓶電壓" in PQ_desc:
                        data[st_uuid]["電瓶電壓"] = pq_uuid
                    if "出水量" in PQ_desc:
                        data[st_uuid]["出水量"] = pq_uuid
                    if "抽水量" in PQ_desc:
                        data[st_uuid]["抽水量"] = pq_uuid
        merge_list.append(data[st_uuid])
    merge_df = pd.DataFrame(merge_list)
    if device_type == 'RFD':
        new_columns = ['st_uuid', 'st_name', '淹水深度', '主機電壓']
        merge_df = merge_df[new_columns]
    elif device_type in ('MPD', 'MPDCY'):
        new_columns = ['st_uuid', 'st_name', '緯度', '經度', '運作狀態',
                       'GPS主機電壓', '電瓶電壓', '抽水量', '出水量']
        merge_df = merge_df[new_columns]
    else:
        first2_col_order = ['st_uuid', 'st_name']
        remaining_col = (merge_df.columns.drop(first2_col_order).tolist())
        new_columns = first2_col_order + remaining_col
        merge_df = merge_df[new_columns]

    f_name = f'{device_type}_最新監測站與物理量UUID對應表.csv'
    f_path = write_file(merge_df, f_name)
    return FileResponse(f_path, media_type='application/octet-stream', filename=f_name)


@router.post("/get_flood_detention_pool")
async def 滯洪池即時水位(
    client_id: str, client_secret: str
):
    # Get IoW token
    PAYLOAD = {"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret}
    response = requests.post("https://iapi.wra.gov.tw/v3/oauth2/token", data=PAYLOAD, timeout=5).json()
    token = response["access_token"]
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}

    st_uuids_path = base_dir + "/STATION_UUIDs/滯洪池_stUUID_pqUUID_滯洪池底.txt"
    with open(st_uuids_path, "r", encoding='utf-8') as f:
        st_uuids_list = f.readlines()

    FLOOD_DETENTION_POOL = defaultdict(dict)
    merge_list = []

    for row in st_uuids_list:
        row = row.strip()
        st_name, town, st_uuid, pq_uuid, pool_depth, levee_height, flood_detention_volume = row.split("\t")
        pool_depth, levee_height, flood_detention_volume = map(float, [pool_depth, levee_height, flood_detention_volume])

        FLOOD_DETENTION_POOL[st_uuid].update({
            "滯洪池名稱": st_name,
            "鄉鎮": town,
            "pq_uuid": pq_uuid,
            "滯洪池底(m)": pool_depth,
            "堤頂高": levee_height,
            "滯洪量(m3)": flood_detention_volume
        })

        # API
        pq_uuid_dict = get_PhysicalQuantity_latest_data(st_uuid, headers, client_id, client_secret)
        if pq_uuid in pq_uuid_dict:
            water_level = pq_uuid_dict[pq_uuid]["Value"]
            FLOOD_DETENTION_POOL[st_uuid]["即時水位(m)"] = water_level
            FLOOD_DETENTION_POOL[st_uuid]["TimeStamp"] = pq_uuid_dict[pq_uuid]["TimeStamp"]

            # 公式： ((堤頂高 - 即時水位)/(堤頂高 - 滯洪池底))*滯洪量*0.7
            estimated_remaining_flood_detention_volume = (
                (levee_height - water_level)/(levee_height - pool_depth) * flood_detention_volume * 0.7
            )
            FLOOD_DETENTION_POOL[st_uuid]["預估剩餘滯洪量(m3)"] = round(estimated_remaining_flood_detention_volume, 1)
        merge_list.append(FLOOD_DETENTION_POOL[st_uuid])

    merge_df = pd.DataFrame(merge_list)
    merge_df = merge_df[
        ['滯洪池名稱', '鄉鎮', '滯洪量(m3)', '預估剩餘滯洪量(m3)', '即時水位(m)', '滯洪池底(m)', '堤頂高', 'TimeStamp']
    ]

    f_name = '滯洪池_即時水位報表.csv'
    f_path = write_file(merge_df, f_name)
    return FileResponse(f_path, media_type='application/octet-stream', filename=f_name)