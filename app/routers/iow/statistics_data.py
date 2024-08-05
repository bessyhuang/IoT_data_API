from fastapi import APIRouter, File, UploadFile
from fastapi.responses import FileResponse

from app.schemas import Pump
from app.routers.iow.latest_data import get_Station_metadata, write_file
from app.routers.iow.history_data import get_PhysicalQuantity_history_data, compress

from decouple import Config, RepositoryEnv
from datetime import timedelta
import pandas as pd
import requests
import os


router = APIRouter()

# Settings
base_dir = os.getcwd()
ENV = Config(RepositoryEnv(base_dir + '/.env'))
PAYLOAD = {
    "grant_type": ENV.get("API_GRANT_TYPE"),
    "client_id": ENV.get("API_CLIENT_ID"),
    "client_secret": ENV.get("API_CLIENT_SECRET"),
}
ignoreNaN = "true"
N_min = 3


def get_country_town_village():
    RFD_url = "https://api.floodsolution.aiot.ing/api/v1/devices/RFD"
    RFD_aiot_data = requests.get(RFD_url).json()
    RFD_aiot_data = RFD_aiot_data['data']

    MPD_url = "https://api.floodsolution.aiot.ing/api/v1/devices/MPD"
    MPD_aiot_data = requests.get(MPD_url).json()
    MPD_aiot_data = MPD_aiot_data['data']

    MPDCY_url = "https://api.floodsolution.aiot.ing/api/v1/devices/MPDCY"
    MPDCY_aiot_data = requests.get(MPDCY_url).json()
    MPDCY_aiot_data = MPDCY_aiot_data['data']

    st_location_dict = dict()
    for st in RFD_aiot_data:
        st_location_dict[st['_id']] = st['county'] + st['town'] + st['village']
    for st in MPD_aiot_data:
        st_location_dict[st['_id']] = st['county'] + st['town'] + st['village']
    for st in MPDCY_aiot_data:
        st_location_dict[st['_id']] = st['county'] + st['town'] + st['village']
    return st_location_dict

def calculate_statistic(pump, df):
    temp_folder_path = base_dir + "/temp/"
    if "Value" not in df:
        group_sums = []
        group_sums.append({
            'st_uuid': pump.st_uuid,
            'pq_uuid': pump.pq_uuid,
            '抽水機編號': pump.st_name,
            '所在地點': pump.location,
            '所屬單位': pump.institution,
            '抽水區間 (Start_TimeStamp)': '',
            '抽水區間 (End_TimeStamp)': '',
            '抽水量(立方公尺)': '',
            '抽水(分)': '此時間區間無歷史資料'
        })
        f_name = F'{temp_folder_path}{pump.st_name}_{pump.datetime_start}_{pump.datetime_end}_output.csv'
        df.to_csv(f_name, encoding='utf-8-sig', index=False)
        result_df = pd.DataFrame(group_sums)
        return result_df
    else:
        df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])
        filtered_data = df[df['Value'] > 0].reset_index(drop=True)

        # Initialize variables to track groups
        groups = []
        current_group = []
        previous_timestamp = None

        # Group consecutive timestamps within 3 mins interval
        for i, row in filtered_data.iterrows():
            if previous_timestamp is None or (row['TimeStamp'] - previous_timestamp <= timedelta(minutes=N_min)):
                current_group.append(row)
            else:
                if current_group:
                    groups.append(current_group)
                current_group = [row]
            previous_timestamp = row['TimeStamp']

        if current_group:
            groups.append(current_group)

        # Sum values in each group
        group_sums = []
        for group in groups:
            sum_values = sum(item['Value'] for item in group)
            group_sums.append({
                'st_uuid': pump.st_uuid,
                'pq_uuid': pump.pq_uuid,
                '抽水機編號': pump.st_name,
                '所在地點': pump.location,
                '所屬單位': pump.institution,
                '抽水區間 (Start_TimeStamp)': group[0]['TimeStamp'],
                '抽水區間 (End_TimeStamp)': group[-1]['TimeStamp'],
                '抽水量(立方公尺)': sum_values,
                '抽水(分)': (group[-1]['TimeStamp'] - group[0]['TimeStamp'])
            })

        result_df = pd.DataFrame(group_sums)
        return result_df


@router.post("/report/pump_runtime", response_class=FileResponse)
async def pump_runtime_report(
        datetime_start: str,
        datetime_end: str,
        st_pq_file: UploadFile = File(...)
    ):
    # Get IoW token
    response = requests.post("https://iapi.wra.gov.tw/v3/oauth2/token", data=PAYLOAD).json()
    token = response["access_token"]
    headers = {"Accept": "application/json", "Authorization": "Bearer %s" % token}

    # Write txt file to temp folder
    try:
        temp_folder_path = base_dir + "/temp/"
        if not os.path.exists(temp_folder_path):
            os.makedirs(temp_folder_path)
        with open(temp_folder_path + st_pq_file.filename, 'wb') as f:
            f.write(st_pq_file.file.read())
    except Exception:
        return {"message": "There was an error uploading the file"}

    # Read txt file (One line, one station)
    with open(temp_folder_path + st_pq_file.filename, 'r') as f:
        content = f.readlines()

    file_names = []
    st_location_dict = get_country_town_village()
    for line in content:
        print(line)
        line = line.replace('\n', '').replace('\t', ',')
        line = line.split(',')
        st_uuid = line[0]
        pq_uuid = line[1]
        try:
            location = st_location_dict[st_uuid]
        except:
            location = ''

        # API
        s_response = get_Station_metadata(st_uuid, headers)
        st_name = s_response['Name']
        institution = s_response["JsonProperties"]["MetaData"]["Institution"]

        pump_item = Pump(
            st_uuid=st_uuid, st_name=st_name, pq_uuid=pq_uuid,
            location=location, institution=institution,
            datetime_start=datetime_start, datetime_end=datetime_end
        )
        df = get_PhysicalQuantity_history_data(headers, pump_item, st_name)
        try:
            df = df[["TimeStamp", "Value"]]
            result_df = calculate_statistic(pump_item, df)
            f_name = F'{st_name}_{pq_uuid}.csv'
            f_path = write_file(result_df, f_name)
            if result_df.shape[0] > 0:
                file_names.append(f_path)
        except:
            pass
    if len(file_names) > 0:
        zip_filepath, f_name = compress(file_names)
        return FileResponse(zip_filepath, media_type='application/octet-stream', filename=f_name)
    else:
        return "No data"