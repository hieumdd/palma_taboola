import os
import json
from datetime import datetime, timedelta
import asyncio

import aiohttp
from google.cloud import bigquery

BASE_URL = "https://backstage.taboola.com/backstage"
ACCOUNT_ID = "palmamedia-network"

DATE_FORMAT = "%Y-%m-%d"

BQ_CLIENT = bigquery.Client()
DATASET = "Palma"


class TopCampaignContent:
    def __init__(self, start=None, end=None):
        self.date_range = self.get_date_range(start, end)

    def get_date_range(self, start, end):
        if start and end:
            start_dt = datetime.strptime(start, DATE_FORMAT)
            end_dt = datetime.strptime(end, DATE_FORMAT)
        else:
            end_dt = datetime.now()
            start_dt = end_dt - timedelta(days=36)

        date_range = []
        while start_dt <= end_dt:
            date_range.append(start_dt.strftime(DATE_FORMAT))
            start_dt = start_dt + timedelta(days=1)
        return date_range

    async def get_headers(self, sessions):
        endpoint = "oauth/token"
        url = f"{BASE_URL}/{endpoint}"
        payload = {
            "client_id": os.getenv("CLIENT_ID"),
            "client_secret": os.getenv("CLIENT_SECRET"),
            "grant_type": "client_credentials",
        }
        headers = {"Content-type": "application/x-www-form-urlencoded"}
        async with sessions.post(url, data=payload, headers=headers) as r:
            res = await r.json()
        access_token = res["access_token"]
        return {
            "Authorization": f"Bearer {access_token}",
            "Content-type": "application/json",
        }

    async def get(self, sessions, headers):
        endpoint = "reports/top-campaign-content/dimensions/item_breakdown"
        url = f"{BASE_URL}/api/1.0/{ACCOUNT_ID}/{endpoint}"
        tasks = [
            asyncio.create_task(self._get(sessions, headers, url, dt))
            for dt in self.date_range
        ]
        rows = await asyncio.gather(*tasks)
        return rows

    async def _get(self, sessions, headers, url, dt):
        params = {"start_date": dt, "end_date": dt}
        async with sessions.get(url, params=params, headers=headers) as r:
            res = await r.json()
        return res

    def transform(self, rows):
        rows = [self._transform(i) for i in rows]
        rows = [item for sublist in rows for item in sublist]
        return rows

    def _transform(self, row):
        results = row["results"]
        _date_range = {
            "start_date": row["metadata"]["start_date"],
            "end_date": row["metadata"]["end_date"],
        }
        _batched_at = {
            "last_used_rawdata_update_time": row["last-used-rawdata-update-time"]
        }
        row = [{**i, **_date_range, **_batched_at} for i in results]
        return row

    def load(self, rows):
        with open("configs/TopCampaignContent.json", "r") as f:
            schema = json.load(f)["schema"]

        loads = BQ_CLIENT.load_table_from_json(
            rows,
            f"{DATASET}._stage_TopCampaignContent",
            job_config=bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
                schema=schema,
            ),
        ).result()

        return loads

    def update(self):
        with open("templates/update_from_stage.sql", "r") as f:
            query = f.read()
        BQ_CLIENT.query(query)

    def run(self):
        return asyncio.run(self._run())

    async def _run(self):
        connector = aiohttp.TCPConnector(limit=20)
        timeout = aiohttp.ClientTimeout(total=540)
        async with aiohttp.ClientSession(
            connector=connector, timeout=timeout
        ) as sessions:
            headers = await self.get_headers(sessions)
            rows = await self.get(sessions, headers)
            rows
            if len(rows) > 0:
                rows = self.transform(rows)
                loads = self.load(rows)
                self.update()
                return {
                    "num_processed": len(rows),
                    "output_rows": loads.output_rows,
                    "errors": loads.errors,
                }
            else:
                return {"num_processed": len(rows)}
