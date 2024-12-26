"""Statistical Data API"""

import os
import pytz
import requests
import pandas as pd
from datetime import datetime, timedelta, time
from decouple import Config, RepositoryEnv

from fastapi import APIRouter, File, UploadFile
from fastapi.responses import FileResponse

from app.schemas import Item, Metadata
from app.routers.iow.latest_data import get_Station_metadata, write_file, get_PhysicalQuantity_latest_data
from app.routers.iow.history_data import (
    get_PhysicalQuantity_history_data,
    get_PhysicalQuantity_history_data_within12hr,
    compress
)


router = APIRouter()
base_dir = os.getcwd()

# Settings
ENV = Config(RepositoryEnv(base_dir + "/.env"))
PAYLOAD = {
    "grant_type": ENV.get("API_GRANT_TYPE"),
    "client_id": ENV.get("API_CLIENT_ID"),
    "client_secret": ENV.get("API_CLIENT_SECRET"),
}

# Get IoW token
response = requests.post("https://iapi.wra.gov.tw/v3/oauth2/token", data=PAYLOAD, timeout=5).json()
token = response["access_token"]
headers = {"Accept": "application/json", "Authorization": "Bearer %s" % token}


def get_country_town_village():
    # Get aiot token
    AIOT_token_url = "https://api.floodsolution.aiot.ing/auth/v1/login"
    body = {
        "username": ENV.get('AIOT_username'),
        "password": ENV.get('AIOT_password')
    }
    AIOT_token = requests.post(AIOT_token_url, json=body, timeout=5).json()["token"]
    headers = {"Accept": "application/json", "Authorization": f"Bearer {AIOT_token}"}

    RFD_url = "https://api.floodsolution.aiot.ing/api/v1/devices/RFD"
    RFD_aiot_data = requests.get(RFD_url, headers=headers, timeout=5).json()
    RFD_aiot_data = RFD_aiot_data["data"]

    MPD_url = "https://api.floodsolution.aiot.ing/api/v1/devices/MPD"
    MPD_aiot_data = requests.get(MPD_url, headers=headers, timeout=5).json()
    MPD_aiot_data = MPD_aiot_data["data"]

    MPDCY_url = "https://api.floodsolution.aiot.ing/api/v1/devices/MPDCY"
    MPDCY_aiot_data = requests.get(MPDCY_url, headers=headers, timeout=5).json()
    MPDCY_aiot_data = MPDCY_aiot_data["data"]

    st_location_dict = dict()
    for st in RFD_aiot_data:
        st_location_dict[st["_id"]] = st["county"] + st["town"] + st["village"]
    for st in MPD_aiot_data:
        st_location_dict[st["_id"]] = st["county"] + st["town"] + st["village"]
    for st in MPDCY_aiot_data:
        st_location_dict[st["_id"]] = st["county"] + st["town"] + st["village"]
    return st_location_dict


def get_date_list(start_time, end_time):
    date_list = []

    # First day (from start_time to start of next day)
    start_of_next_day = pytz.timezone('Asia/Taipei').localize(
        datetime.combine(start_time.date() + timedelta(days=1), datetime.min.time())
    )
    date_list.append((start_time, start_of_next_day))

    # Middle days (full: day1 00:00:00 to day2 00:00:00)
    current_date = start_time.date() + timedelta(days=1)
    while current_date < end_time.date():
        start_of_day = pytz.timezone('Asia/Taipei').localize(
            datetime.combine(current_date, datetime.min.time())
        )
        end_of_day = pytz.timezone('Asia/Taipei').localize(
            datetime.combine(current_date + timedelta(days=1), datetime.min.time())
        )
        date_list.append((start_of_day, end_of_day))
        current_date += timedelta(days=1)

    # Last day (from start of the day to end_time)
    start_of_last_day = pytz.timezone('Asia/Taipei').localize(
        datetime.combine(end_time.date(), datetime.min.time())
    )
    date_list.append((start_of_last_day, end_time))
    return date_list


def transform_avail_pump(pump, df, data_time):
    if "Value" not in df:
        if data_time < pump.datetime_start:
            return {
                "st_uuid": pump.st_uuid,
                "pq_uuid": pump.pq_uuid,
                "st_name": pump.st_name,
                "近12小時的時間區間": f"{pump.datetime_start} ~ {pump.datetime_end}",
                "近12小時是否曾抽水": "無資料",
                "近12小時是否有歷史資料": "無資料",
                "最近一筆資料的上傳時間": data_time,
                "是否可被調度": "Y"
            }
        else:
            return {
                "st_uuid": pump.st_uuid,
                "pq_uuid": "",
                "st_name": pump.st_name,
                "近12小時的時間區間": f"{pump.datetime_start} ~ {pump.datetime_end}",
                "近12小時是否曾抽水": "無資料",
                "近12小時是否有歷史資料": "無資料",
                "最近一筆資料的上傳時間": data_time,
                "是否可被調度": "Y"
            }

    if (df["Value"] > 0).any():
        return {
            "st_uuid": pump.st_uuid,
            "pq_uuid": pump.pq_uuid,
            "st_name": pump.st_name,
            "近12小時的時間區間": f"{pump.datetime_start} ~ {pump.datetime_end}",
            "近12小時是否曾抽水": "Y",
            "近12小時是否有歷史資料": "Y",
            "最近一筆資料的上傳時間": data_time,
            "是否可被調度": "N"
        }
    else:
        return {
            "st_uuid": pump.st_uuid,
            "pq_uuid": pump.pq_uuid,
            "st_name": pump.st_name,
            "近12小時的時間區間": f"{pump.datetime_start} ~ {pump.datetime_end}",
            "近12小時是否曾抽水": "N",
            "近12小時是否有歷史資料": "Y",
            "最近一筆資料的上傳時間": data_time,
            "是否可被調度": "Y"
        }


def calculate_pump_runtime(
    pump, df, pump_interval_N_min
):
    if "Value" not in df:
        return pd.DataFrame([
            {
                "st_uuid": pump.st_uuid,
                "pq_uuid": pump.pq_uuid,
                "抽水機編號": pump.st_name,
                "所在地點": pump.location,
                "所屬單位": pump.institution,
                "抽水區間 (Start_TimeStamp)": "",
                "抽水區間 (End_TimeStamp)": "",
                "抽水量(立方公尺)": "",
                "抽水(分)": "此時間區間無歷史資料",
            }
        ])

    # Initialize variables to track groups
    df["TimeStamp"] = pd.to_datetime(df["TimeStamp"], format="ISO8601")
    filtered_data = df[df["Value"] > 0].reset_index(drop=True)

    groups = []
    current_group = []
    prev_timestamp = None

    # Group consecutive timestamps within N mins interval
    for i, row in filtered_data.iterrows():
        if (prev_timestamp is None) or (row["TimeStamp"] - prev_timestamp <= timedelta(minutes=pump_interval_N_min)):
            current_group.append(row)
        else:
            if current_group:
                groups.append(current_group)
            current_group = [row]
        prev_timestamp = row["TimeStamp"]

    if current_group:
        groups.append(current_group)

    # Sum values in each group
    group_sums = []
    for group in groups:
        start_time = group[0]["TimeStamp"]
        end_time = group[-1]["TimeStamp"]

        # Handle cases where group spans multiple days
        if start_time.date() != end_time.date():
            date_list = get_date_list(start_time, end_time)
            for date_interval in date_list:
                date_1, date_2 = date_interval
                duration_1 = (date_2 - date_1).total_seconds()
                pumping_volume_1 = duration_1 * 0.3

                # 略過不計：抽水小於等於10秒(抽水量小於等於3)
                if pumping_volume_1 > 3:
                    group_sums.append(
                        {
                            "st_uuid": pump.st_uuid,
                            "pq_uuid": pump.pq_uuid,
                            "抽水機編號": pump.st_name,
                            "所在地點": pump.location,
                            "所屬單位": pump.institution,
                            "抽水區間 (Start_TimeStamp)": date_1,
                            "抽水區間 (End_TimeStamp)": date_2,
                            "抽水量(立方公尺)": pumping_volume_1,
                            "抽水(分)": timedelta(seconds=duration_1),
                        }
                    )
        else:
            # Single interval case (no date boundary crossing)
            duration_sec = (end_time - start_time).total_seconds()
            pumping_volume = duration_sec * 0.3

            # 略過不計：抽水小於等於10秒(抽水量小於等於3)
            if pumping_volume > 3:
                group_sums.append(
                    {
                        "st_uuid": pump.st_uuid,
                        "pq_uuid": pump.pq_uuid,
                        "抽水機編號": pump.st_name,
                        "所在地點": pump.location,
                        "所屬單位": pump.institution,
                        "抽水區間 (Start_TimeStamp)": start_time,
                        "抽水區間 (End_TimeStamp)": end_time,
                        "抽水量(立方公尺)": pumping_volume,
                        "抽水(分)": timedelta(seconds=duration_sec),
                    }
                )

    if group_sums == []:
        group_sums.append(
            {
                "st_uuid": pump.st_uuid,
                "pq_uuid": pump.pq_uuid,
                "抽水機編號": pump.st_name,
                "所在地點": pump.location,
                "所屬單位": pump.institution,
                "抽水區間 (Start_TimeStamp)": "",
                "抽水區間 (End_TimeStamp)": "",
                "抽水量(立方公尺)": "",
                "抽水(分)": "此時間區間_無抽水",
            }
        )
    return pd.DataFrame(group_sums)


def calculate_avail_rate(item, df, N_records_per_day):
    start_date = datetime.strptime(item.datetime_end, "%Y-%m-%d")
    end_date = datetime.strptime(item.datetime_start, "%Y-%m-%d")
    duration_in_days = (start_date - end_date).days + 1
    if "TimeStamp" in df.columns:
        df["Date"] = pd.to_datetime(df.TimeStamp, format="mixed").dt.strftime("%Y-%m-%d")
        df["DateTime"] = pd.to_datetime(df.TimeStamp, format="mixed").dt.strftime("%Y-%m-%d %H:%M")
        df_cal = df.groupby(["Date"]).size().reset_index(name="counts")
        print(df_cal)

        # 日妥善率 (%)     = [N (筆/天)]   除以   [24 (預期一天應該要有24筆，RULE_DayFrequency)]  乘以 [100 %]
        daily_avg_field_name = f'日妥善率 (%) = counts / {N_records_per_day}'
        df_cal[daily_avg_field_name] = df_cal["counts"] / N_records_per_day * 100
        df_cal[daily_avg_field_name] = df_cal[daily_avg_field_name].apply(
            lambda x: 100 if x >= 100 else x
        )

        if duration_in_days >= 28:
            # 月平均妥善率 (%) = [每天筆數相加] 除以   [30*24 (預期30天應該要有30*24筆)]           乘以 [100 %]
            monthly_avg_field_name = f'月平均妥善率 (%) = sum(日妥善率) / ({duration_in_days}天 * 100)'
            df_cal[monthly_avg_field_name] = sum(df_cal[daily_avg_field_name]) / (duration_in_days * 100) * 100
            df_cal[monthly_avg_field_name] = df_cal[monthly_avg_field_name].apply(
                lambda x: 100 if x >= 100 else x
            )
        return df_cal


def calculate_max_flood_height(pump, df, flood_height_interval_N_min):
    if "Value" not in df:
        group_sums = []
        group_sums.append(
            {
                "st_uuid": pump.st_uuid,
                "pq_uuid": pump.pq_uuid,
                "水位站編號": pump.st_name,
                "所在地點": pump.location,
                "所屬單位": pump.institution,
                "淹水區間 (Start_TimeStamp)": "",
                "淹水區間 (End_TimeStamp)": "",
                "最大淹水高度(公分)": "",
                "淹水持續時間(分鐘)": "此時間區間無歷史資料",
            }
        )
        result_df = pd.DataFrame(group_sums)
        return result_df

    # Initialize variables to track groups
    groups = []
    current_group = []
    prev_timestamp = None
    df["TimeStamp"] = pd.to_datetime(df["TimeStamp"], format="ISO8601")
    filtered_data = df[df["Value"] > 0].reset_index(drop=True)

    # Group consecutive timestamps within 3 mins interval
    for i, row in filtered_data.iterrows():
        if (prev_timestamp is None) or (row["TimeStamp"] - prev_timestamp <= timedelta(minutes=flood_height_interval_N_min)):
            current_group.append(row)
        else:
            if current_group:
                groups.append(current_group)
            current_group = [row]
        prev_timestamp = row["TimeStamp"]

    if current_group:
        groups.append(current_group)

    # Sum values in each group
    group_sums = []
    for group in groups:
        max_values = max(item["Value"] for item in group)
        if max_values > 0:
            group_sums.append(
                {
                    "st_uuid": pump.st_uuid,
                    "pq_uuid": pump.pq_uuid,
                    "水位站編號": pump.st_name,
                    "所在地點": pump.location,
                    "所屬單位": pump.institution,
                    "淹水區間 (Start_TimeStamp)": group[0]["TimeStamp"],
                    "淹水區間 (End_TimeStamp)": group[-1]["TimeStamp"],
                    "最大淹水高度(公分)": max_values,
                    "淹水持續時間(分鐘)": (
                        group[-1]["TimeStamp"] - group[0]["TimeStamp"]
                    ),
                }
            )

    if group_sums == []:
        group_sums.append(
            {
                "st_uuid": pump.st_uuid,
                "pq_uuid": pump.pq_uuid,
                "水位站編號": pump.st_name,
                "所在地點": pump.location,
                "所屬單位": pump.institution,
                "淹水區間 (Start_TimeStamp)": "",
                "淹水區間 (End_TimeStamp)": "",
                "最大淹水高度(公分)": "",
                "淹水持續時間(分鐘)": "此時間區間無淹水",
            }
        )
    result_df = pd.DataFrame(group_sums)
    return result_df


def calculate_opsUnits_pumpingVol(concat_df):
    # Group by Date
    concat_df["日期"] = pd.to_datetime(
        concat_df["抽水區間 (Start_TimeStamp)"], format="mixed"
    ).dt.strftime("%Y-%m-%d")
    new_df = concat_df.groupby("日期").agg(
        {"抽水機編號": "nunique", "抽水量(立方公尺)": "sum"}
    )
    new_df = new_df.reset_index()
    new_df = new_df.rename(columns={"抽水機編號": "運轉台數"})
    new_df.loc["Total"] = new_df.sum()
    new_df.loc[new_df.index[-1], "運轉台數"] = None
    new_df.loc[new_df.index[-1], "日期"] = "合計"
    return new_df


@router.post("/report/pump_runtime", response_class=FileResponse)
async def 抽水區間報表(
    datetime_start: str,
    datetime_end: str,
    pump_interval_N_min: int = 10,
    st_pq_file: UploadFile = File(...)
):
    # Get IoW token
    response = requests.post("https://iapi.wra.gov.tw/v3/oauth2/token", data=PAYLOAD, timeout=5).json()
    token = response["access_token"]
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}

    # Write txt file to temp folder
    try:
        temp_folder_path = base_dir + "/temp/"
        if not os.path.exists(temp_folder_path):
            os.makedirs(temp_folder_path)
        with open(temp_folder_path + st_pq_file.filename, "wb") as f:
            f.write(st_pq_file.file.read())
    except Exception as e:
        return {"message": f"There was an error uploading the file. {str(e)}"}

    # Read txt file (One line, one station)
    with open(temp_folder_path + st_pq_file.filename, "r", encoding="utf-8") as f:
        content = f.readlines()

    file_names = []
    st_location_dict = get_country_town_village()
    for line in content:
        line = line.replace("\n", "").replace("\t", ",")
        line = line.split(",")
        st_uuid = line[0]
        pq_uuid = line[1]

        # API
        s_response = get_Station_metadata(st_uuid, headers, PAYLOAD["client_id"], PAYLOAD["client_secret"])
        st_name = s_response["Name"]

        pump_item = Metadata(
            st_uuid=st_uuid,
            st_name=st_name,
            pq_uuid=pq_uuid,
            location=st_location_dict[st_uuid],
            institution=s_response["JsonProperties"]["MetaData"]["Institution"],
            datetime_start=datetime_start,
            datetime_end=datetime_end,
        )
        df = get_PhysicalQuantity_history_data(headers, PAYLOAD["client_id"], PAYLOAD["client_secret"], pump_item, st_name)
        result_df = calculate_pump_runtime(pump_item, df, pump_interval_N_min)
        f_name = f'{st_name}_{pq_uuid}.csv'
        f_path = write_file(result_df, f_name)
        if result_df.shape[0] > 0:
            file_names.append(f_path)

    pd_list = []
    for f_st_pump_runtime in file_names:
        df_st_pump_runtime = pd.read_csv(f_st_pump_runtime)
        pd_list.append(df_st_pump_runtime)
    concat_df = pd.concat(pd_list)
    f_name = "合併_抽水區間報表.csv"
    f_path = write_file(concat_df, f_name)
    return FileResponse(f_path, media_type="application/octet-stream", filename=f_name)


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
        with open(temp_folder_path + st_pq_file.filename, "wb") as f:
            f.write(st_pq_file.file.read())
    except Exception as e:
        return {"message": f"There was an error uploading the file. {str(e)}"}

    # Read txt file (One line, one station & one physical quantity)
    with open(temp_folder_path + st_pq_file.filename, "r", encoding="utf-8") as f:
        content = f.readlines()

    file_names = []
    for line in content:
        line = line.replace("\n", "").replace("\t", ",")
        line = line.split(",")
        st_uuid = line[0]
        pq_uuid = line[1]

        # API
        s_response = get_Station_metadata(st_uuid, headers, PAYLOAD["client_id"], PAYLOAD["client_secret"])
        st_name = s_response["Name"]

        item = Item(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            st_uuid=st_uuid,
            pq_uuid=pq_uuid,
        )
        df = get_PhysicalQuantity_history_data(headers, PAYLOAD["client_id"], PAYLOAD["client_secret"], item, st_name)
        try:
            df = df[["TimeStamp", "Value"]]
            result_df = calculate_avail_rate(item, df, N_records_per_day)
            f_name = f'{st_name}_妥善率_summary.csv'
            f_path = write_file(result_df, f_name)
            if result_df.shape[0] > 0:
                file_names.append(f_path)
        except Exception as e:
            with open(temp_folder_path + "無歷史資料的監測站_AvailRate_report.txt", "a", encoding="utf-8") as f:
                f.write(f'{st_name}\t{st_uuid}\n')

    if os.path.isfile(temp_folder_path + "無歷史資料的監測站_AvailRate_report.txt"):
        file_names.append(temp_folder_path + "無歷史資料的監測站_AvailRate_report.txt")

    if len(file_names) > 0:
        zip_filepath, f_name = compress(file_names)
        return FileResponse(zip_filepath, media_type="application/octet-stream", filename=f_name)
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
    response = requests.post("https://iapi.wra.gov.tw/v3/oauth2/token", data=PAYLOAD, timeout=5).json()
    token = response["access_token"]
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}

    # Write txt file to temp folder
    try:
        temp_folder_path = base_dir + "/temp/"
        if not os.path.exists(temp_folder_path):
            os.makedirs(temp_folder_path)
        with open(temp_folder_path + st_pq_file.filename, "wb") as f:
            f.write(st_pq_file.file.read())
    except Exception as e:
        return {"message": f"There was an error uploading the file. {str(e)}"}

    # Read txt file (One line, one station)
    with open(temp_folder_path + st_pq_file.filename, "r", encoding="utf-8") as f:
        content = f.readlines()

    file_names = []
    st_location_dict = get_country_town_village()
    for line in content:
        line = line.replace("\n", "").replace("\t", ",")
        line = line.split(",")
        st_uuid = line[0]
        pq_uuid = line[1]

        # API
        s_response = get_Station_metadata(st_uuid, headers, PAYLOAD["client_id"], PAYLOAD["client_secret"])
        st_name = s_response["Name"]

        rfd_item = Metadata(
            st_uuid=st_uuid,
            st_name=st_name,
            pq_uuid=pq_uuid,
            location=st_location_dict[st_uuid],
            institution=s_response["JsonProperties"]["MetaData"]["Institution"],
            datetime_start=datetime_start,
            datetime_end=datetime_end,
        )
        df = get_PhysicalQuantity_history_data(headers, PAYLOAD["client_id"], PAYLOAD["client_secret"], rfd_item, st_name)
        result_df = calculate_max_flood_height(rfd_item, df, flood_height_interval_N_min)
        f_name = f'{st_name}_{pq_uuid}.csv'
        f_path = write_file(result_df, f_name)
        if result_df.shape[0] > 0:
            file_names.append(f_path)

    pd_list = []
    for f_st_max_flood_height in file_names:
        df_st_max_flood_height = pd.read_csv(f_st_max_flood_height)
        pd_list.append(df_st_max_flood_height)
    concat_df = pd.concat(pd_list)
    f_name = "合併_最大淹水高度區間報表.csv"
    f_path = write_file(concat_df, f_name)
    return FileResponse(f_path, media_type="application/octet-stream", filename=f_name)


@router.post("/report/operating_units_and_pumping_volumes", response_class=FileResponse)
async def 運轉台數與抽水量的即時報表(
    datetime_start: str,
    datetime_end: str,
    pump_interval_N_min: int = 10,
    st_pq_file: UploadFile = File(...)
):
    # Get IoW token
    response = requests.post("https://iapi.wra.gov.tw/v3/oauth2/token", data=PAYLOAD, timeout=5).json()
    token = response["access_token"]
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}

    # Write txt file to temp folder
    try:
        temp_folder_path = base_dir + "/temp/"
        if not os.path.exists(temp_folder_path):
            os.makedirs(temp_folder_path)
        with open(temp_folder_path + st_pq_file.filename, "wb") as f:
            f.write(st_pq_file.file.read())
    except Exception as e:
        return {"message": f"There was an error uploading the file. {str(e)}"}

    # Read txt file (One line, one station)
    with open(temp_folder_path + st_pq_file.filename, "r", encoding="utf-8") as f:
        content = f.readlines()

    file_names = []
    for line in content:
        line = line.replace("\n", "").replace("\t", ",")
        line = line.split(",")
        st_uuid = line[0]
        pq_uuid = line[1]

        # API
        s_response = get_Station_metadata(st_uuid, headers, PAYLOAD["client_id"], PAYLOAD["client_secret"])
        st_name = s_response["Name"]

        pump_item = Metadata(
            st_uuid=st_uuid,
            st_name=st_name,
            pq_uuid=pq_uuid,
            datetime_start=datetime_start,
            datetime_end=datetime_end,
        )
        df = get_PhysicalQuantity_history_data(headers, PAYLOAD["client_id"], PAYLOAD["client_secret"], pump_item, st_name)
        try:
            df = df[["TimeStamp", "Value"]]
            result_df = calculate_pump_runtime(pump_item, df, pump_interval_N_min)
            f_name = f'{st_name}_{pq_uuid}.csv'
            f_path = write_file(result_df, f_name)
            if result_df.shape[0] > 0:
                file_names.append(f_path)
        except Exception as e:
            with open(temp_folder_path + "無歷史資料的監測站_OperatingUnits_and_PumpingVolumes_report.txt", "a", encoding="utf-8") as f_out:
                f_out.write(f'{st_name}\t{st_uuid}\n')

    pd_list = []
    for f_st_pump_runtime in file_names:
        df_st_pump_runtime = pd.read_csv(f_st_pump_runtime)
        pd_list.append(df_st_pump_runtime)
    concat_df = pd.concat(pd_list)
    result_df = calculate_opsUnits_pumpingVol(concat_df)
    f_name = "MPD_MPDCY_移動式抽水機_運轉台數及抽水量報表.csv"
    f_path = write_file(result_df, f_name)
    file_names.append(f_path)

    if os.path.isfile(temp_folder_path + "無歷史資料的監測站_OperatingUnits_and_PumpingVolumes_report.txt"):
        file_names.append(temp_folder_path + "無歷史資料的監測站_OperatingUnits_and_PumpingVolumes_report.txt")

    if len(file_names) > 0:
        zip_filepath, f_name = compress(file_names)
        return FileResponse(zip_filepath, media_type="application/octet-stream", filename=f_name)
    else:
        result = {"msg": "NO DATA TO DOWNLOAD"}
        return result


@router.post("/report/available_pumps", response_class=FileResponse)
async def 可調度抽水機的即時報表():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Get aiot token
    AIOT_token_url = "https://api.floodsolution.aiot.ing/auth/v1/login"
    body = {
        "username": ENV.get('AIOT_username'),
        "password": ENV.get('AIOT_password')
    }
    AIOT_token = requests.post(AIOT_token_url, json=body, timeout=5).json()["token"]
    headers = {"Accept": "application/json", "Authorization": f"Bearer {AIOT_token}"}

    MPD_url = "https://api.floodsolution.aiot.ing/api/v1/devices/MPD"
    MPD_aiot_data = requests.get(MPD_url, headers=headers, timeout=5).json()
    MPD_aiot_data = MPD_aiot_data["data"]

    MPDCY_url = "https://api.floodsolution.aiot.ing/api/v1/devices/MPDCY"
    MPDCY_aiot_data = requests.get(MPDCY_url, headers=headers, timeout=5).json()
    MPDCY_aiot_data = MPDCY_aiot_data["data"]

    st_dict = {}
    for st in MPD_aiot_data:
        # 1 (未出勤)、2 (出勤中)、3 (抽水中)、4 (運送中)、5 (異常)
        if (st["detailStatus"] == 1) or (st["detailStatus"] == 2):
            st_dict[st["_id"]] = {
                "st_name": st["name"],
                "st_uuid": st["_id"],
                "lat": st["lat"],
                "lon": st["lon"],
                "location": st["county"] + st["town"] + st["village"],
                "dev_type": st["type"],
            }
    for st in MPDCY_aiot_data:
        if (st["detailStatus"] == 1) or (st["detailStatus"] == 2):
            st_dict[st["_id"]] = {
                "st_name": st["name"],
                "st_uuid": st["_id"],
                "lat": st["lat"],
                "lon": st["lon"],
                "location": st["county"] + st["town"] + st["village"],
                "dev_type": st["type"],
            }

    df = pd.DataFrame.from_dict(st_dict, orient="index")
    f_name = f'可調度的抽水機報表_{now}.csv'
    f_path = write_file(df, f_name)
    return FileResponse(f_path, media_type="application/octet-stream", filename=f_name)


@router.post("/report/available_pumps_within12hr", response_class=FileResponse)
async def 十二小時內無抽水紀錄_可調度抽水機的即時報表():
    now = datetime.now()
    start_time = pytz.timezone('Asia/Taipei').localize(
        datetime.combine(now - timedelta(days=1), time(now.hour, 0, 0))
        ).strftime("%Y-%m-%dT%H.00.00")
    end_time = datetime.now().strftime("%Y-%m-%dT%H.00.00")

    # STATION_UUIDs (get from local)
    st_uuids_path = base_dir + "/STATION_UUIDs/MPD_MPDCY_all_info.csv"  #
    df = pd.read_csv(st_uuids_path, encoding='utf-8')

    # Get IoW token
    response = requests.post("https://iapi.wra.gov.tw/v3/oauth2/token", data=PAYLOAD, timeout=5).json()
    token = response["access_token"]
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}

    st_dict = {}
    for index, row in df.iterrows():
        if (pd.isna(row['抽水量']) == True) and (pd.isna(row['出水量']) == True):
            data = {"Station": row['st_name']}
            df = pd.DataFrame(data, index=[0])
            pq_uuid = row['經度']
            pump_item = Metadata(
                st_uuid=row['st_uuid'],
                st_name=row['st_name'],
                pq_uuid=pq_uuid,
                datetime_start=start_time,
                datetime_end=end_time,
            )
        else:
            if pd.isna(row['抽水量']) == False:
                pq_uuid = row['抽水量']
            elif pd.isna(row['出水量']) == False:
                pq_uuid = row['出水量']
            pump_item = Metadata(
                st_uuid=row['st_uuid'],
                st_name=row['st_name'],
                pq_uuid=pq_uuid,
                datetime_start=start_time,
                datetime_end=end_time,
            )
            df = get_PhysicalQuantity_history_data_within12hr(
                headers, PAYLOAD["client_id"], PAYLOAD["client_secret"], pump_item, row['st_name']
            )
        pq_uuid_dict = get_PhysicalQuantity_latest_data(
            row['st_uuid'], headers, PAYLOAD["client_id"], PAYLOAD["client_secret"]
        )
        data_time = pq_uuid_dict[pq_uuid]["TimeStamp"].replace("+08:00", "")
        result_dict = transform_avail_pump(pump_item, df, data_time)
        st_dict[row['st_uuid']] = result_dict
    avail_pump_df = pd.DataFrame.from_dict(st_dict, orient="index")
    f_name = f'十二小時內無抽水紀錄_可調度的抽水機報表_{now}.csv'
    f_path = write_file(avail_pump_df, f_name)
    return FileResponse(f_path, media_type="application/octet-stream", filename=f_name)
