import os
from datetime import datetime, timedelta
import asyncio
from abc import ABC, abstractmethod

import requests
import aiohttp

API_VER = "1.0"
BASE_URL = "https://backstage.taboola.com/backstage"
ACCOUNT_ID = "palmamedia-network"

NOW = datetime.utcnow()
DATE_FORMAT = "%Y-%m-%d"


def get_headers():
    with requests.post(
        url=f"{BASE_URL}/oauth/token",
        data={
            "client_id": os.getenv("CLIENT_ID"),
            "client_secret": os.getenv("CLIENT_SECRET"),
            "grant_type": "client_credentials",
        },
        headers={
            "Content-type": "application/x-www-form-urlencoded",
        },
    ) as r:
        res = r.json()
    return {
        "Authorization": f"Bearer {res['access_token']}",
        "Content-type": "application/json",
    }


def get_date_range(start, end):
    if start and end:
        _start, _end = [datetime.strptime(i, DATE_FORMAT) for i in (start, end)]
    else:
        _start, _end = [NOW - timedelta(days=36), NOW]
    return _start, _end


class Getter(ABC):
    def __init__(self, model):
        self.start, self.end = get_date_range(model.start, model.end)
        self.endpoint = model.endpoint
        self.url = f"{BASE_URL}/api/{API_VER}/{ACCOUNT_ID}/{self.endpoint}"
        self.headers = get_headers()

    @abstractmethod
    def get(self):
        pass


class MultiDayGetter(Getter):
    def get(self):
        with requests.get(
            self.url,
            params={
                "start_date": self.start.strftime(DATE_FORMAT),
                "end_date": self.end.strftime(DATE_FORMAT),
            },
            headers=self.headers,
        ) as r:
            res = r.json()
        return res


class SingleDayGetter(Getter):
    def get(self):
        return asyncio.run(self._get())

    async def _get(self):
        date_range = [
            self.start + timedelta(i)
            for i in range(int((self.end - self.start).days) + 1)
        ]
        connector = aiohttp.TCPConnector(limit=10)
        timeout = aiohttp.ClientTimeout(total=540)
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
        ) as sessions:
            tasks = [
                asyncio.create_task(self._get_one(sessions, self.url, dt))
                for dt in date_range
            ]
            rows = await asyncio.gather(*tasks)
        return rows

    async def _get_one(self, sessions, url, dt):
        params = self._get_params(dt)
        async with sessions.get(url, params=params, headers=self.headers) as r:
            res = await r.json()
        return res

    @abstractmethod
    def _get_params(self, dt):
        pass


class OneDayGetter(SingleDayGetter):
    def _get_params(self, dt):
        return {
            "start_date": dt.strftime(DATE_FORMAT),
            "end_date": (dt + timedelta(days=1)).strftime(DATE_FORMAT),
        }


class FullDayGetter(SingleDayGetter):
    def _get_params(self, dt):
        return {
            "start_date": dt.strftime(DATE_FORMAT),
            "end_date": dt.strftime(DATE_FORMAT),
        }