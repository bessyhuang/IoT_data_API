from fastapi import APIRouter, File, UploadFile
from fastapi.responses import FileResponse

from app.schemas import Item, Pump
from app.routers.iow.latest_data import get_Station_metadata, write_file
from app.routers.iow.history_data import get_PhysicalQuantity_history_data, compress

from decouple import Config, RepositoryEnv
from datetime import datetime as dt
from datetime import timedelta
import pandas as pd
import requests
import os


router = APIRouter()
base_dir = os.getcwd()

# Settings
ENV = Config(RepositoryEnv(base_dir + '/.env'))
PAYLOAD = {
    "grant_type": ENV.get("API_GRANT_TYPE"),
    "client_id": ENV.get("API_CLIENT_ID"),
    "client_secret": ENV.get("API_CLIENT_SECRET"),
}

# Get IoW token
response = requests.post("https://iapi.wra.gov.tw/v3/oauth2/token", data=PAYLOAD).json()
token = response["access_token"]
headers = {"Accept": "application/json", "Authorization": "Bearer %s" % token}


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

def calculate_pump_runtime(pump, df, pump_interval_N_min):
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
        result_df = pd.DataFrame(group_sums)
        return result_df
    else:
        df['TimeStamp'] = pd.to_datetime(df['TimeStamp'], format='ISO8601')
        filtered_data = df[df['Value'] > 0].reset_index(drop=True)

        # Initialize variables to track groups
        groups = []
        current_group = []
        previous_timestamp = None

        # Group consecutive timestamps within 3 mins interval
        for i, row in filtered_data.iterrows():
            if (previous_timestamp is None) or (row['TimeStamp'] - previous_timestamp <= timedelta(minutes=pump_interval_N_min)):
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
            duration_sec = (group[-1]['TimeStamp'] - group[0]['TimeStamp']).total_seconds()
            pumping_volume = duration_sec * 0.3
            # sum_values = sum(item['Value'] for item in group)  # remove rule
            if pumping_volume > 0:
                group_sums.append({
                    'st_uuid': pump.st_uuid,
                    'pq_uuid': pump.pq_uuid,
                    '抽水機編號': pump.st_name,
                    '所在地點': pump.location,
                    '所屬單位': pump.institution,
                    '抽水區間 (Start_TimeStamp)': group[0]['TimeStamp'],
                    '抽水區間 (End_TimeStamp)': group[-1]['TimeStamp'],
                    '抽水量(立方公尺)': pumping_volume,
                    '抽水(分)': (group[-1]['TimeStamp'] - group[0]['TimeStamp'])
                })

        if group_sums == []:
            group_sums.append({
                'st_uuid': pump.st_uuid,
                'pq_uuid': pump.pq_uuid,
                '抽水機編號': pump.st_name,
                '所在地點': pump.location,
                '所屬單位': pump.institution,
                '抽水區間 (Start_TimeStamp)': '',
                '抽水區間 (End_TimeStamp)': '',
                '抽水量(立方公尺)': '',
                '抽水(分)': '此時間區間_無抽水'
            })
            result_df = pd.DataFrame(group_sums)
            return result_df
        else:
            result_df = pd.DataFrame(group_sums)
            return result_df

def calculate_avail_rate(item, df, N_records_per_day):
    duration_in_days = (dt.strptime(item.datetime_end, '%Y-%m-%d') - dt.strptime(item.datetime_start, '%Y-%m-%d')).days + 1
    if 'TimeStamp' in df.columns:
        df['Date'] = pd.to_datetime(df.TimeStamp, format='mixed').dt.strftime('%Y-%m-%d')
        df['DateTime'] = pd.to_datetime(df.TimeStamp, format='mixed').dt.strftime('%Y-%m-%d %H:%M')
        df_cal = df.groupby(['Date']).size().reset_index(name='counts')

        # 日妥善率 (%)     = [N (筆/天)]   除以   [24 (預期一天應該要有24筆，RULE_DayFrequency)]  乘以 [100 %]
        daily_avg_field_name = F'日妥善率 (%) = counts / {N_records_per_day}'
        df_cal[daily_avg_field_name] = df_cal['counts'] / N_records_per_day * 100
        df_cal[daily_avg_field_name] = df_cal[daily_avg_field_name].apply(lambda x: 100 if x >= 100 else x)

        if duration_in_days >= 28:
            # 月平均妥善率 (%) = [每天筆數相加] 除以   [30*24 (預期30天應該要有30*24筆)]           乘以 [100 %]
            monthly_avg_field_name = F'月平均妥善率 (%) = sum(日妥善率) / ({duration_in_days}天 * 100)'
            df_cal[monthly_avg_field_name] = sum(df_cal[daily_avg_field_name]) / (duration_in_days * 100) * 100
            df_cal[monthly_avg_field_name] = df_cal[monthly_avg_field_name].apply(lambda x: 100 if x >= 100 else x)
        return df_cal

def calculate_max_flood_height(pump, df, flood_height_interval_N_min):
    if "Value" not in df:
        group_sums = []
        group_sums.append({
            'st_uuid': pump.st_uuid,
            'pq_uuid': pump.pq_uuid,
            '水位站編號': pump.st_name,
            '所在地點': pump.location,
            '所屬單位': pump.institution,
            '淹水區間 (Start_TimeStamp)': '',
            '淹水區間 (End_TimeStamp)': '',
            '最大淹水高度(公分)': '',
            '淹水持續時間(分鐘)': '此時間區間無歷史資料'
        })
        result_df = pd.DataFrame(group_sums)
        return result_df
    else:
        df['TimeStamp'] = pd.to_datetime(df['TimeStamp'], format='ISO8601')
        filtered_data = df[df['Value'] > 0].reset_index(drop=True)

        # Initialize variables to track groups
        groups = []
        current_group = []
        previous_timestamp = None

        # Group consecutive timestamps within 3 mins interval
        for i, row in filtered_data.iterrows():
            if (previous_timestamp is None) or (row['TimeStamp'] - previous_timestamp <= timedelta(minutes=flood_height_interval_N_min)):
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
            max_values = max(item['Value'] for item in group)
            if max_values > 0:
                group_sums.append({
                    'st_uuid': pump.st_uuid,
                    'pq_uuid': pump.pq_uuid,
                    '水位站編號': pump.st_name,
                    '所在地點': pump.location,
                    '所屬單位': pump.institution,
                    '淹水區間 (Start_TimeStamp)': group[0]['TimeStamp'],
                    '淹水區間 (End_TimeStamp)': group[-1]['TimeStamp'],
                    '最大淹水高度(公分)': max_values,
                    '淹水持續時間(分鐘)': (group[-1]['TimeStamp'] - group[0]['TimeStamp'])
                })

        if group_sums == []:
            group_sums.append({
                'st_uuid': pump.st_uuid,
                'pq_uuid': pump.pq_uuid,
                '水位站編號': pump.st_name,
                '所在地點': pump.location,
                '所屬單位': pump.institution,
                '淹水區間 (Start_TimeStamp)': '',
                '淹水區間 (End_TimeStamp)': '',
                '最大淹水高度(公分)': '',
                '淹水持續時間(分鐘)': '此時間區間無淹水'
            })
            result_df = pd.DataFrame(group_sums)
            return result_df
        else:
            result_df = pd.DataFrame(group_sums)
            return result_df

def calculate_opsUnits_pumpingVol(concat_df):
    # Group by Date
    concat_df['日期'] = pd.to_datetime(concat_df['抽水區間 (Start_TimeStamp)'], format='mixed').dt.strftime('%Y-%m-%d')
    new_df = concat_df.groupby('日期').agg({'抽水機編號': 'nunique', '抽水量(立方公尺)': 'sum'})
    new_df = new_df.reset_index()
    new_df = new_df.rename(columns={'抽水機編號': '運轉台數'})
    # new_df.loc['Total'] = new_df.sum(numeric_only=True, axis=0)
    new_df.loc['Total'] = new_df.sum()
    new_df.loc[new_df.index[-1], '運轉台數'] = ''
    new_df.loc[new_df.index[-1], '日期'] = '合計'
    return new_df

@router.post("/report/pump_runtime", response_class=FileResponse)
async def 抽水區間報表(
    datetime_start: str,
    datetime_end: str,
    pump_interval_N_min: int = 10,
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
        result_df = calculate_pump_runtime(pump_item, df, pump_interval_N_min)
        f_name = F'{st_name}_{pq_uuid}.csv'
        f_path = write_file(result_df, f_name)
        if result_df.shape[0] > 0:
            file_names.append(f_path)

    pdList = []
    for f_st_pump_runtime in file_names:
        df_st_pump_runtime = pd.read_csv(f_st_pump_runtime)
        pdList.append(df_st_pump_runtime)
    concat_df = pd.concat(pdList)
    f_name = '合併_抽水區間報表.csv'
    f_path = write_file(concat_df, f_name)
    return FileResponse(f_path, media_type='application/octet-stream', filename=f_name)

@router.post("/report/avail_rate", response_class=FileResponse)
async def 日和月平均妥善率報表(
    datetime_start: str,
    datetime_end: str,
    N_records_per_day: int = 24,
    st_pq_file: UploadFile = File(...)
):
    # Write txt file to temp folder
    try:
        temp_folder_path = base_dir + "/temp/"
        if not os.path.exists(temp_folder_path):
            os.makedirs(temp_folder_path)
        with open(temp_folder_path + st_pq_file.filename, 'wb') as f:
            f.write(st_pq_file.file.read())
    except Exception:
        return {"message": "There was an error uploading the file"}

    # Read txt file (One line, one station & one physical quantity)
    with open(temp_folder_path + st_pq_file.filename, 'r') as f:
        content = f.readlines()

    file_names = []
    for line in content:
        line = line.replace('\n', '').replace('\t', ',')
        line = line.split(',')
        st_uuid = line[0]
        pq_uuid = line[1]

        # API
        s_response = get_Station_metadata(st_uuid, headers)
        st_name = s_response['Name']

        All_pq_Dict = dict()
        All_pq_Dict[pq_uuid] = []
        item = Item(datetime_start=datetime_start, datetime_end=datetime_end, st_uuid=st_uuid, pq_uuid=pq_uuid)
        df = get_PhysicalQuantity_history_data(headers, item, st_name)
        try:
            df = df[["TimeStamp", "Value"]]
            result_df = calculate_avail_rate(item, df, N_records_per_day)
            f_name = F'{st_name}_妥善率_summary.csv'
            f_path = write_file(result_df, f_name)
            if result_df.shape[0] > 0:
                file_names.append(f_path)
        except:
            with open(temp_folder_path + '無歷史資料的監測站_AvailRate_report.txt', 'a') as f_out:
                f_out.write(F"{st_name}\t{st_uuid}\n")

    if os.path.isfile(temp_folder_path + '無歷史資料的監測站_AvailRate_report.txt'):
        file_names.append(temp_folder_path + '無歷史資料的監測站_AvailRate_report.txt')

    if len(file_names) > 0:
        zip_filepath, f_name = compress(file_names)
        os.system(F"rm {temp_folder_path}無歷史資料的監測站_AvailRate_report.txt")
        return FileResponse(zip_filepath, media_type='application/octet-stream', filename=f_name)
    else:
        result = {"msg": "NO DATA TO DOWNLOAD"}
        return result

@router.post("/report/max_flood_height", response_class=FileResponse)
async def 最大淹水高度區間報表(
    datetime_start: str,
    datetime_end: str,
    flood_height_interval_N_min: int = 8,
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

        waterlevel_item = Pump(
            st_uuid=st_uuid, st_name=st_name, pq_uuid=pq_uuid,
            location=location, institution=institution,
            datetime_start=datetime_start, datetime_end=datetime_end
        )
        df = get_PhysicalQuantity_history_data(headers, waterlevel_item, st_name)
        result_df = calculate_max_flood_height(waterlevel_item, df, flood_height_interval_N_min)
        f_name = F'{st_name}_{pq_uuid}.csv'
        f_path = write_file(result_df, f_name)
        if result_df.shape[0] > 0:
            file_names.append(f_path)

    pdList = []
    for f_st_max_flood_height in file_names:
        df_st_max_flood_height = pd.read_csv(f_st_max_flood_height)
        pdList.append(df_st_max_flood_height)
    concat_df = pd.concat(pdList)
    f_name = '合併_最大淹水高度區間報表.csv'
    f_path = write_file(concat_df, f_name)
    return FileResponse(f_path, media_type='application/octet-stream', filename=f_name)

@router.post("/report/operating_units_and_pumping_volumes", response_class=FileResponse)
async def 運轉台數與抽水量的即時報表(
    datetime_start: str,
    datetime_end: str,
    pump_interval_N_min: int = 10,
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
    for line in content:
        line = line.replace('\n', '').replace('\t', ',')
        line = line.split(',')
        st_uuid = line[0]
        pq_uuid = line[1]

        # API
        s_response = get_Station_metadata(st_uuid, headers)
        st_name = s_response['Name']

        pump_item = Pump(
            st_uuid=st_uuid, st_name=st_name, pq_uuid=pq_uuid,
            datetime_start=datetime_start, datetime_end=datetime_end
        )
        df = get_PhysicalQuantity_history_data(headers, pump_item, st_name)
        try:
            df = df[["TimeStamp", "Value"]]
            result_df = calculate_pump_runtime(pump_item, df, pump_interval_N_min)
            f_name = F'{st_name}_{pq_uuid}.csv'
            f_path = write_file(result_df, f_name)
            if result_df.shape[0] > 0:
                file_names.append(f_path)
        except:
            with open(temp_folder_path + '無歷史資料的監測站_OperatingUnits_and_PumpingVolumes_report.txt', 'a') as f_out:
                f_out.write(F"{st_name}\t{st_uuid}\n")

    pdList = []
    for f_st_pump_runtime in file_names:
        df_st_pump_runtime = pd.read_csv(f_st_pump_runtime)
        pdList.append(df_st_pump_runtime)
    concat_df = pd.concat(pdList)
    result_df = calculate_opsUnits_pumpingVol(concat_df)
    f_name = 'MPD_MPDCY_移動式抽水機_運轉台數及抽水量報表.csv'
    f_path = write_file(result_df, f_name)
    file_names.append(f_path)

    if os.path.isfile(temp_folder_path + '無歷史資料的監測站_OperatingUnits_and_PumpingVolumes_report.txt'):
        file_names.append(temp_folder_path + '無歷史資料的監測站_OperatingUnits_and_PumpingVolumes_report.txt')

    if len(file_names) > 0:
        zip_filepath, f_name = compress(file_names)
        os.system(F"rm {temp_folder_path}無歷史資料的監測站_OperatingUnits_and_PumpingVolumes_report.txt")
        return FileResponse(zip_filepath, media_type='application/octet-stream', filename=f_name)
    else:
        result = {"msg": "NO DATA TO DOWNLOAD"}
        return result

@router.post("/report/available_pumps", response_class=FileResponse)
async def 可調度抽水機的即時報表():
    now = dt.now().strftime('%Y-%m-%d %H:%M:%S')

    MPD_url = "https://api.floodsolution.aiot.ing/api/v1/devices/MPD"
    MPD_aiot_data = requests.get(MPD_url).json()
    MPD_aiot_data = MPD_aiot_data['data']

    MPDCY_url = "https://api.floodsolution.aiot.ing/api/v1/devices/MPDCY"
    MPDCY_aiot_data = requests.get(MPDCY_url).json()
    MPDCY_aiot_data = MPDCY_aiot_data['data']

    st_dict = dict()
    for st in MPD_aiot_data:
        # 1 (未出勤)、2 (出勤中)、3 (抽水中)、4 (運送中)、5 (異常)
        if (st['detailStatus'] == 1) or (st['detailStatus'] == 2):
            st_dict[st['_id']] = {
                "st_name": st['name'],
                "st_uuid": st['_id'],
                "lat": st['lat'],
                "lon": st['lon'],
                "location": st['county'] + st['town'] + st['village'],
                "dev_type": st['type']
            }
    for st in MPDCY_aiot_data:
        if (st['detailStatus'] == 1) or (st['detailStatus'] == 2):
            st_dict[st['_id']] = {
                "st_name": st['name'],
                "st_uuid": st['_id'],
                "lat": st['lat'],
                "lon": st['lon'],
                "location": st['county'] + st['town'] + st['village'],
                "dev_type": st['type']
            }

    df = pd.DataFrame.from_dict(st_dict, orient='index')
    f_name = F'可調度的抽水機報表_{now}.csv'
    f_path = write_file(df, f_name)
    return FileResponse(f_path, media_type='application/octet-stream', filename=f_name)