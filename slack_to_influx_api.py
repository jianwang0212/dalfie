import pandas as pd
import re
import numpy as np
from datetime import datetime
import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from web3 import Web3
import os
from pathlib import Path
from dotenv import load_dotenv
from slack_sdk import WebClient
import pandas as pd
import numpy as np


# web3 prepare 
price_mapping = {"Biswap":1.55,
                 "Mdex":0.0004} 

token_mapping = {"Biswap":"0x965F527D9159dCe6288a2219DB51fc6Eef120dD1",
                 "Mdex":"0x56DAdC6699A08adA499c93c8d6B4f5fB6094a6D0"}                 
tokenAddress = "0x965F527D9159dCe6288a2219DB51fc6Eef120dD1" 
myContractAddress = "0xaa209927bb214ab7Df9bA0368da81B091d327307"
tokenAbi = [{"inputs":[],"payable":False,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"owner","type":"address"},{"indexed":True,"internalType":"address","name":"spender","type":"address"},{"indexed":False,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":True,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"from","type":"address"},{"indexed":True,"internalType":"address","name":"to","type":"address"},{"indexed":False,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"constant":True,"inputs":[],"name":"_decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":True,"inputs":[],"name":"_name","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":True,"inputs":[],"name":"_symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":True,"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":False,"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":True,"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":False,"inputs":[{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"burn","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":True,"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":False,"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"subtractedValue","type":"uint256"}],"name":"decreaseAllowance","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":True,"inputs":[],"name":"getOwner","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":False,"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"addedValue","type":"uint256"}],"name":"increaseAllowance","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":False,"inputs":[{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"mint","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":True,"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":True,"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":False,"inputs":[],"name":"renounceOwnership","outputs":[],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":True,"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":True,"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":False,"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":False,"inputs":[{"internalType":"address","name":"sender","type":"address"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":False,"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"payable":False,"stateMutability":"nonpayable","type":"function"}]



# slack read  
env_path = Path('.') / '.env'

load_dotenv(dotenv_path = env_path)

client = WebClient(token = os.environ['SLACK_TOKEN'])

result = client.conversations_history(channel="C01MS3ENTDL")

r = result['messages']

df = pd.DataFrame(r)

df1 = df[df['bot_id'] == 'B01NBFX8AMP']
time_slack = df1.iloc[0]['ts']
t2 = df1.iloc[0]['text']
t1 = df1.iloc[1]['text']
t = t1 + t2
df2 = pd.DataFrame([x.split(';') for x in t.split('\n')])
df2.columns = ['c1']
df3 = df2[df2['c1']!= "Dalfie's been trading away:"].reset_index()
df4 = df3['c1'].str.replace('*','', regex = False)

file = df4.to_frame()
name =file[file['c1'].str.match(r'(\w{2,8}-)')] 
# this is to get the trader name
name = name.append(file.iloc[-1])
print(f'number of traders: {name.shape[0]}')

df = pd.DataFrame()
for i in range(name.shape[0]-1):
    item = file.iloc[name.index[i]:name.index[i+1]].reset_index()
    item = item.rename(columns = {"c1":i})
    df = pd.concat([df,item.T])
    i +=1
df = df.iloc[1::2]
df.columns = ['name', 'ccy1','ccy2','bal_total', 'pnl_daily','pnl_weekly','pnl_monthly']
df['name'] =df['name'].str.split(':').str[0]

df[['ccy1','bal_ccy1']] = df['ccy1'].str.split(":", n=1, expand = True)
df[['ccy2','bal_ccy2']] = df['ccy2'].str.split(":", n=1, expand = True)

df[['bal_total_ccy','bal_total']] = df['bal_total'].str.split(":", n=1, expand = True)
df['bal_total_ccy'] = df['bal_total_ccy'].str.extract(r'.*\((.*)\).*')

df['bal_ccy2'] = df['bal_ccy2'].astype(float) 
df['bal_ccy1'] = df['bal_ccy1'].astype(float)
df['bal_total'] = df['bal_total'].astype(float)

df['pnl_daily_pct'] = df['pnl_daily'].str.split(" ").str[1]
df['pnl_daily_pct'] = df['pnl_daily_pct'].str.rstrip("%").astype("float")/100
df['pnl_weekly_pct'] = df['pnl_weekly'].str.split(" ").str[1]
df['pnl_weekly_pct'] = df['pnl_weekly_pct'].str.rstrip("%").astype("float")/100
df['pnl_monthly_pct'] = df['pnl_monthly'].str.split(" ").str[1]
df['pnl_monthly_pct'] = df['pnl_monthly_pct'].str.rstrip("%").astype("float")/100

# traders using usd
df['usd_pricing'] =df['bal_total_ccy'].str.contains("usd|dai")

df['pnl_daily_str'] = df['pnl_daily'].str.extract(r'.*\((.*)\).*')
df['pnl_daily_num'] = df['pnl_daily_str'].str.split(" ").str[0].astype(float)

df['pnl_weekly_str'] = df['pnl_weekly'].str.extract(r'.*\((.*)\).*')
df['pnl_weekly_num'] = df['pnl_weekly_str'].str.split(" ").str[0].astype(float)

df['pnl_monthly_str'] = df['pnl_monthly'].str.extract(r'.*\((.*)\).*')
df['pnl_monthly_num'] = df['pnl_monthly_str'].str.split(" ").str[0].astype(float)

print(f"number of usd_pricing traders: {df[df['usd_pricing']].shape[0]}")
print(f"monthly pnl of all usd_pricing traders: {df[df['usd_pricing']]['pnl_monthly_num'].sum()}")

df['ccy1_ccy2_price'] = (df['bal_total'] - df['bal_ccy2']) / df['bal_ccy1']
df.replace([np.inf, -np.inf], np.nan, inplace=True)
df['ccy1_ccy2_price'] = df['ccy1_ccy2_price'].round(1)

# address mapping
address = pd.read_csv("address_trader_mapping.csv")

df2 = address[["name", "Trader","Smart contract"]]

# small chain mapping
exchange = pd.read_csv("chain_exchange_mapping.csv")

df3 = exchange[["name",  "Chain", "bal_small_coin", "bal_in", "coin", "exchange"]]


# merge 
df_final = df.merge(df2, on = "name")
df_final = df_final.merge(df3, on = "name")



df_final['tokenAddress'] = df_final['exchange'].map(token_mapping)

df_final['bal_address'] = df_final.apply(lambda x: x['Trader'] if x['bal_in'] == 'a' else x['Smart contract'], axis = 1)

# get the bal for alt coins


def get_bal(tokenAddress,tokenAbi, myContractAddress, chain):
    if chain == "BSC":
        url = "https://speedy-nodes-nyc.moralis.io/316cd5651c670ee2fadac742/bsc/mainnet"
    elif chain == "HECO":
        url = "https://http-mainnet.hecochain.com"
        tokenAddress = Web3.toChecksumAddress(tokenAddress)
    elif chain == "ETH":
        url = "https://mainnet.infura.io/v3/875e7a523df844e08a050acc035a6891"
    else:
        return 0

    web3 = Web3(Web3.HTTPProvider(url))
    try: 
        web3.eth.contract(address = tokenAddress,abi =tokenAbi)
    except:
        return 0
    
    contractToken = web3.eth.contract(address = tokenAddress,abi =tokenAbi)

    try: 
        contractToken.functions.balanceOf(myContractAddress).call()
    except:
        return 0
    bal = contractToken.functions.balanceOf(myContractAddress).call()
    output = float(web3.fromWei(bal,"ether"))
    return output

df_final['alt_bal'] = df_final.apply(lambda x: get_bal(x['tokenAddress'], tokenAbi,x['bal_address'], x['Chain']), axis = 1)

df_final.to_csv('output_new.csv')


# write to influx 
df = df_final
df['time_now'] = datetime.fromtimestamp(float(time_slack))
print(datetime.fromtimestamp(float(time_slack)))
df.set_index('time_now',inplace = True)
tags=['name','Chain','exchange','usd_pricing','ccy1','ccy2']
df = df.tz_localize('Asia/Shanghai')
# You can generate an API token from the "API Tokens Tab" in the UI
token = "GrfxS5qkPdY4rSPeYDSzE3VvzRPtR9Ik4BNkmhvhJ12q-utviJtxYWdAaxSCUnzUqV3-D-_MrHiy2E0-I3bzHA=="
org = "zi_org"
bucket = "my-bucket"

with InfluxDBClient(url="http://68.183.38.145:8086", token=token, org=org) as client:
    _write_client = client.write_api(write_options=SYNCHRONOUS)
    _write_client.write(bucket, org, record=df, data_frame_measurement_name='linux_server', data_frame_tag_columns = tags)

