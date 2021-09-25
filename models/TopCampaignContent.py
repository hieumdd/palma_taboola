from models.models import Taboola
from components.getter import FullDayGetter


class TopCampaignContent(Taboola):
    table = "TopCampaignContent"
    endpoint = "reports/top-campaign-content/dimensions/item_breakdown"
    getter = FullDayGetter
    keys = {
        "p_key": [
            "item",
            "campaign",
            "content_provider",
            "start_date",
            "end_date",
        ],
        "incre_key": "last_used_rawdata_update_time",
    }
    schema = [
        {"name": "item", "type": "STRING"},
        {"name": "item_name", "type": "STRING"},
        {"name": "description", "type": "STRING"},
        {"name": "thumbnail_url", "type": "STRING"},
        {"name": "url", "type": "STRING"},
        {"name": "campaign", "type": "STRING"},
        {"name": "campaign_name", "type": "STRING"},
        {"name": "content_provider", "type": "STRING"},
        {"name": "content_provider_name", "type": "STRING"},
        {"name": "impressions", "type": "INTEGER"},
        {"name": "visible_impressions", "type": "INTEGER"},
        {"name": "ctr", "type": "FLOAT"},
        {"name": "vctr", "type": "FLOAT"},
        {"name": "clicks", "type": "INTEGER"},
        {"name": "cpc", "type": "FLOAT"},
        {"name": "cvr", "type": "FLOAT"},
        {"name": "cpa", "type": "FLOAT"},
        {"name": "actions", "type": "INTEGER"},
        {"name": "cpm", "type": "FLOAT"},
        {"name": "vcpm", "type": "FLOAT"},
        {"name": "spent", "type": "FLOAT"},
        {"name": "conversions_value", "type": "FLOAT"},
        {"name": "roas", "type": "FLOAT"},
        {"name": "currency", "type": "STRING"},
        {"name": "learning_display_status", "type": "STRING"},
        {"name": "start_date", "type": "TIMESTAMP"},
        {"name": "end_date", "type": "TIMESTAMP"},
        {"name": "last_used_rawdata_update_time", "type": "TIMESTAMP"},
    ]

    def _transform(self, results):
        rows = [self._transform_one(i) for i in results]
        return [item for sublist in rows for item in sublist]

    def _transform_one(self, result):
        rows = result["results"]
        return [
            {
                "item": row["item"],
                "item_name": row["item_name"],
                "description": row["description"],
                "thumbnail_url": row["thumbnail_url"],
                "url": row["url"],
                "campaign": row["campaign"],
                "campaign_name": row["campaign_name"],
                "content_provider": row["content_provider"],
                "content_provider_name": row["content_provider_name"],
                "impressions": row["impressions"],
                "visible_impressions": row["visible_impressions"],
                "ctr": row["ctr"],
                "vctr": row["vctr"],
                "clicks": row["clicks"],
                "cpc": row["cpc"],
                "cvr": row["cvr"],
                "cpa": row["cpa"],
                "actions": row["actions"],
                "cpm": row["cpm"],
                "vcpm": row["vcpm"],
                "spent": row["spent"],
                "conversions_value": row["conversions_value"],
                "roas": row["roas"],
                "currency": row["currency"],
                "learning_display_status": row["learning_display_status"],
                "last_used_rawdata_update_time": result[
                    "last-used-rawdata-update-time"
                ],
                "start_date": result["metadata"]["start_date"],
                "end_date": result["metadata"]["end_date"],
            }
            for row in rows
        ]
