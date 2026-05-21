from datetime import datetime
import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import os
df = pd.read_csv(os.getcwd() + "/one_week_gas——bsc.csv")
df2 = df[['timeStamp', 'hash','exchange','blockNumber','gas_fee']]
df2.set_index('timeStamp',inplace=True)
df2.index = pd.to_datetime(df2.index, unit = "s", utc=True)


# You can generate an API token from the "API Tokens Tab" in the UI
token = os.environ.get("INFLUXDB_TOKEN")
org = os.environ.get("INFLUXDB_ORG", "zi_org")
bucket = os.environ.get("INFLUXDB_BUCKET", "my-bucket")

url = os.environ.get("INFLUXDB_URL", "http://68.183.38.145:8086")
if not token:
    raise SystemExit("Set INFLUXDB_TOKEN before running this script.")

with InfluxDBClient(url=url, token=token, org=org) as client:
    _write_client = client.write_api(write_options=SYNCHRONOUS)
    _write_client.write(bucket, org, record=df2, data_frame_measurement_name='web12345')
