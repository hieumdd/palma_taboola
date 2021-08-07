import os
import json
from datetime import datetime, timedelta
import asyncio
from abc import ABC, abstractmethod

import requests
import aiohttp
from google.cloud import bigquery
import jinja2

API_VER = "1.0"
BASE_URL = "https://backstage.taboola.com/backstage"
ACCOUNT_ID = "palmamedia-network"

NOW = datetime.utcnow()
DATE_FORMAT = "%Y-%m-%d"

BQ_CLIENT = bigquery.Client()
DATASET = "Palma"

TEMPLATE_LOADER = jinja2.FileSystemLoader("./templates")
TEMPLATE_ENV = jinja2.Environment(loader=TEMPLATE_LOADER)


class Getter(ABC):
    def __init__(self, start, end, endpoint):
        self.start = start
        self.end = end
        self.endpoint = endpoint
        self.url = f"{BASE_URL}/api/{API_VER}/{ACCOUNT_ID}/{self.endpoint}"
        self.headers = self.get_headers()

    @staticmethod
    def get_headers():
        endpoint = "oauth/token"
        url = f"{BASE_URL}/{endpoint}"
        payload = {
            "client_id": os.getenv("CLIENT_ID"),
            "client_secret": os.getenv("CLIENT_SECRET"),
            "grant_type": "client_credentials",
        }
        headers = {"Content-type": "application/x-www-form-urlencoded"}
        with requests.post(url, data=payload, headers=headers) as r:
            res = r.json()
        access_token = res["access_token"]
        return {
            "Authorization": f"Bearer {access_token}",
            "Content-type": "application/json",
        }

    @abstractmethod
    def get(self):
        pass


class MultiDayGetter(Getter):
    def __init__(self, start, end, endpoint):
        super().__init__(start, end, endpoint)

    def get(self):
        params = {
            "start_date": self.start.strftime(DATE_FORMAT),
            "end_date": self.end.strftime(DATE_FORMAT),
        }
        with requests.get(self.url, params=params, headers=self.headers) as r:
            res = r.json()
        return res


class SingleDayGetter(Getter):
    def __init__(self, start, end, endpoint):
        super().__init__(start, end, endpoint)

    def get(self):
        return asyncio.run(self._get())

    async def _get(self):
        date_range = []
        start = self.start
        while start <= self.end:
            date_range.append(start)
            start = start + timedelta(days=1)

        connector = aiohttp.TCPConnector(limit=10)
        timeout = aiohttp.ClientTimeout(total=540)
        async with aiohttp.ClientSession(
            connector=connector, timeout=timeout
        ) as sessions:
            tasks = [
                asyncio.create_task(self._get_one(sessions, self.url, dt))
                for dt in date_range
            ]
            rows = await asyncio.gather(*tasks)
        return rows

    async def _get_one(self, sessions, url, dt):
        params = self.get_params(dt)
        async with sessions.get(url, params=params, headers=self.headers) as r:
            res = await r.json()
        return res

    @abstractmethod
    def get_params(self, dt):
        pass

class OneDayGetter(SingleDayGetter):
    def __init__(self, start, end, endpoint):
        super().__init__(start, end, endpoint)

    def get_params(self, dt):
        return {
            "start_date": dt.strftime(DATE_FORMAT),
            "end_date": (dt + timedelta(days=1)).strftime(DATE_FORMAT),
        }

class FullDayGetter(SingleDayGetter):
    def __init__(self, start, end, endpoint):
        super().__init__(start, end, endpoint)

    def get_params(self, dt):
        return {
            "start_date": dt.strftime(DATE_FORMAT),
            "end_date": dt.strftime(DATE_FORMAT),
        }


class Transformer(ABC):
    @abstractmethod
    def transform(self):
        pass


class SingleDayTransformer(Transformer):
    def transform(self, rows):
        rows = [self._transform(i) for i in rows]
        rows = [item for sublist in rows for item in sublist]
        return rows

    def _transform(self, row):
        results = row["results"]
        row = [
            {
                **i,
                "start_date": row["metadata"]["start_date"],
                "end_date": row["metadata"]["end_date"],
                "last_used_rawdata_update_time": row["last-used-rawdata-update-time"],
            }
            for i in results
        ]
        return row


class MultiDayTransformer(Transformer):
    def transform(self, rows):
        results = rows["results"]
        row = [
            {
                **i,
                "last_used_rawdata_update_time": rows["last-used-rawdata-update-time"],
            }
            for i in results
        ]
        return row


class Taboola(ABC):
    @staticmethod
    def factory(table, start, end):
        args = (start, end)
        if table == "TopCampaignContent":
            return TopCampaignContent(*args)
        elif table == "CampaignSummary":
            return CampaignSummary(*args)
        elif table == "CampaignSummaryHourly":
            return CampaignSummaryHourly(*args)

    def __init__(self, start, end):
        self.start, self.end = self.get_date_range(start, end)
        self.keys, self.schema = self.get_config()

    @staticmethod
    def get_date_range(_start, _end):
        if _start and _end:
            start = datetime.strptime(_start, DATE_FORMAT)
            end = datetime.strptime(_end, DATE_FORMAT)
        else:
            start = NOW - timedelta(days=36)
            end = NOW
        return start, end

    def get_config(self):
        with open(f"configs/{self.table}.json", "r") as f:
            config = json.load(f)
        return config["keys"], config["schema"]

    def load(self, rows):
        return BQ_CLIENT.load_table_from_json(
            rows,
            f"{DATASET}._stage_{self.table}",
            job_config=bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
                schema=self.schema,
            ),
        ).result()

    def update(self):
        template = TEMPLATE_ENV.get_template("update_from_stage.sql.j2")
        rendered_query = template.render(
            dataset=DATASET,
            table=self.table,
            p_key=",".join(self.keys.get("p_key")),
            incre_key=self.keys.get("incre_key"),
        )
        BQ_CLIENT.query(rendered_query)

    def run(self):
        rows = self.getter.get()
        responses = {
            "table": self.table,
            "start": self.start.strftime(DATE_FORMAT),
            "end": self.end.strftime(DATE_FORMAT),
        }
        if len(rows) > 0:
            rows = self.transformer.transform(rows)
            loads = self.load(rows)
            self.update()
            responses = {
                **responses,
                "num_processed": len(rows),
                "output_rows": loads.output_rows,
            }
        return responses


class TopCampaignContent(Taboola):
    table = "TopCampaignContent"

    def __init__(self, start, end):
        super().__init__(start, end)
        self.getter = FullDayGetter(
            self.start,
            self.end,
            "reports/top-campaign-content/dimensions/item_breakdown",
        )
        self.transformer = SingleDayTransformer()


class CampaignSummary(Taboola):
    table = "CampaignSummary"

    def __init__(self, start, end):
        super().__init__(start, end)
        self.getter = MultiDayGetter(
            self.start,
            self.end,
            "reports/campaign-summary/dimensions/campaign_site_day_breakdown",
        )
        self.transformer = MultiDayTransformer()


class CampaignSummaryHourly(Taboola):
    table = "CampaignSummaryHourly"

    def __init__(self, start, end):
        super().__init__(start, end)
        self.getter = OneDayGetter(
            self.start,
            self.end,
            "reports/campaign-summary/dimensions/campaign_hour_breakdown",
        )
        self.transformer = SingleDayTransformer()
