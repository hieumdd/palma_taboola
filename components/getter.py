import os
import json
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
        if isinstance(start, datetime):
            _start, _end = start, end
        else:
            _start, _end = [datetime.strptime(i, DATE_FORMAT) for i in (start, end)]
    else:
        _start, _end = [NOW - timedelta(days=36), NOW]
    return _start, _end


class Getter(ABC):
    def __init__(self, start, end, endpoint):
        self.start, self.end = get_date_range(start, end)
        self.endpoint = endpoint
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
        ) as session:
            tasks = [
                asyncio.create_task(self._get_one(session, dt)) for dt in date_range
            ]
            rows = await asyncio.gather(*tasks)
        return rows

    async def _get_one(self, session, dt, attempt=0):
        try:
            async with session.get(
                self.url,
                params=self._get_params(dt),
                headers=self.headers,
            ) as r:
                res = await r.json()
        except aiohttp.client_exceptions.ContentTypeError as e:
            if attempt < 5:
                asyncio.sleep(2)
                return await self._get_one(session, dt, attempt + 1)
            else:
                raise e
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


class CampaignFilterGetter(Getter):
    def get(self):
        campaign_date = self._get_campaign_date()
        return asyncio.run(self._get_data(campaign_date))

    def _get_campaign_date(self):
        multi_day_getter = MultiDayGetter(
            self.start,
            self.end,
            "reports/campaign-summary/dimensions/campaign_day_breakdown",
        )
        results = multi_day_getter.get()["results"]
        return [
            {
                "date": datetime.strptime(i["date"], "%Y-%m-%d %H:%M:%S.%f"),
                "campaign": i["campaign"],
            }
            for i in results
        ]

    async def _get_data(self, campaign_date):
        connector = aiohttp.TCPConnector(limit=10)
        timeout = aiohttp.ClientTimeout(total=3600)
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
        ) as session:
            tasks = [
                asyncio.create_task(self._get_one(session, i)) for i in campaign_date
            ]
            rows = await asyncio.gather(*tasks)
        return rows

    async def _get_one(self, session, filter_, attempt=0):
        try:
            async with session.get(
                self.url,
                params={
                    "start_date": filter_["date"].strftime(DATE_FORMAT),
                    "end_date": filter_["date"].strftime(DATE_FORMAT),
                    "campaign": filter_["campaign"],
                },
                headers=self.headers,
            ) as r:
                res = await r.json()
                return {
                    **res,
                    "campaign": filter_["campaign"],
                }
        except aiohttp.client_exceptions.ContentTypeError as e:
            if attempt < 5:
                asyncio.sleep(2)
                return await self._get_one(session, filter_, attempt + 1)
            else:
                raise e
