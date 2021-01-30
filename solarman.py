import argparse
from datetime import datetime, date
from getpass import getpass
import pandas as pd
import sys
import aiohttp
import asyncio
from hashlib import sha256


def get_config():
    parser = argparse.ArgumentParser(
        description='Solarman data downloader app')
    parser.add_argument('-u', '--username', required=True,
                        help='Solarman account username')
    parser.add_argument('-s', '--start-date',
                        required=True,
                        type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
                        help="Start date in format % Y-%m-%d, eg: 2020-01-25")
    parser.add_argument('-e', '--end-date',
                        type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
                        default=datetime.today(),
                        help=("End date in format % Y-%m-%d, "
                              "eg: 2020-01-25, default=today"))
    parser.add_argument('-o', '--output',
                        default=None,
                        help=("Output filename (csv). default=stdout"))
    args = parser.parse_args()

    if sys.stdin.isatty():
        password = getpass(f'Solarman password for user {args.username}: ')
    else:
        password = sys.stdin.readline().rstrip()
    return (args.username, password, args.start_date,
            args.end_date, args.output)


async def get_token(username, clear_text_pwd, org_id=None):
    h = sha256()
    h.update(clear_text_pwd.encode())
    password = h.hexdigest()

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "password",
        "identity_type": 2,
        "username": f"{username}",
        "password": f"{password}",
        "clear_text_pwd": f"{clear_text_pwd}",
        "client_id": "test",
    }
    if org_id:
        data["org_id"] = org_id
    url = "https://login-pro.solarmanpv.com/oauth-s/oauth/token"
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=data, headers=headers) as response:
            data = await response.json()
            return data['access_token']


async def get_org_id(token):
    headers = {
        "Authorization": f"Bearer {token}",
    }

    url = 'https://login-pro.solarmanpv.com/user-s/acc/org/my'
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            data = await response.json()
            return data[0]['org']['id']


async def get_pv_data(token, year, month):
    url = "https://pro.solarmanpv.com/maintain-s/history/power/stats/month"
    headers = {
        "Content-Type": "application/json;charset=utf-8",
        "Authorization": f"Bearer {token}",
    }
    params = {'year': f'{year}', 'month': f'{month}'}

    data = '{}'

    async with aiohttp.ClientSession() as session:
        async with session.post(
                url, headers=headers, params=params, data=data) as response:
            return (await response.json())


async def main():
    username, clear_text_pwd, start_date, end_date, output = get_config()
    # Login first to get access_token
    token = await get_token(username, clear_text_pwd)
    # Using bearer token get org_id
    org_id = await get_org_id(token)
    # Get new token based on org_id
    token = await get_token(username, clear_text_pwd, org_id)

    # Finally we can download data
    end_date = datetime.today()
    dates_range = pd.date_range(start_date, end_date, freq='MS').tolist()
    data = await asyncio.gather(
        *(get_pv_data(token, dt.year, dt.month) for dt in dates_range)

    )
    df_data = []
    for monthly_data in data:
        for daily_data in monthly_data["items"]:
            df_data.append({
                "date": date(daily_data["year"],
                             daily_data["month"],
                             daily_data["day"]),
                "kWh": daily_data["generationValue"],
                "full_power_hours": daily_data["fullPowerHoursDay"]
            })
    df = pd.DataFrame(df_data)
    res = df.to_csv(output, index=False)
    if res:
        print(res)

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
