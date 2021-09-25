from models.models import Taboola
from components.getter import MultiDayGetter


class CampaignSummary(Taboola):
    table = "CampaignSummary"
    endpoint = "reports/campaign-summary/dimensions/campaign_site_day_breakdown"
    getter = MultiDayGetter
    keys = {
        "p_key": [
            "date",
            "campaign",
            "campaign_name",
            "site",
            "site_name",
        ],
        "incre_key": "last_used_rawdata_update_time",
    }
    schema = [
        {"name": "date", "type": "TIMESTAMP"},
        {"name": "site", "type": "STRING"},
        {"name": "site_name", "type": "STRING"},
        {"name": "site_id", "type": "INTEGER"},
        {"name": "campaign", "type": "STRING"},
        {"name": "campaign_name", "type": "STRING"},
        {"name": "clicks", "type": "INTEGER"},
        {"name": "impressions", "type": "INTEGER"},
        {"name": "visible_impressions", "type": "INTEGER"},
        {"name": "spent", "type": "FLOAT"},
        {"name": "conversions_value", "type": "FLOAT"},
        {"name": "roas", "type": "FLOAT"},
        {"name": "ctr", "type": "FLOAT"},
        {"name": "vctr", "type": "FLOAT"},
        {"name": "cpm", "type": "FLOAT"},
        {"name": "vcpm", "type": "FLOAT"},
        {"name": "cpc", "type": "FLOAT"},
        {"name": "cpa", "type": "FLOAT"},
        {"name": "cpa_actions_num", "type": "INTEGER"},
        {"name": "cpa_conversion_rate", "type": "FLOAT"},
        {"name": "blocking_level", "type": "STRING"},
        {"name": "currency", "type": "STRING"},
        {"name": "last_used_rawdata_update_time", "type": "TIMESTAMP"},
    ]

    def _transform(self, results):
        rows = results["results"]
        return [
            {
                "date": row["date"],
                "site": row["site"],
                "site_name": row["site_name"],
                "site_id": row["site_id"],
                "campaign": row["campaign"],
                "campaign_name": row["campaign_name"],
                "clicks": row["clicks"],
                "impressions": row["impressions"],
                "visible_impressions": row["visible_impressions"],
                "spent": row["spent"],
                "conversions_value": row["conversions_value"],
                "roas": row["roas"],
                "ctr": row["ctr"],
                "vctr": row["vctr"],
                "cpm": row["cpm"],
                "vcpm": row["vcpm"],
                "cpc": row["cpc"],
                "cpa": row["cpa"],
                "cpa_actions_num": row["cpa_actions_num"],
                "cpa_conversion_rate": row["cpa_conversion_rate"],
                "blocking_level": row["blocking_level"],
                "currency": row["currency"],
                "last_used_rawdata_update_time": results[
                    "last-used-rawdata-update-time"
                ],
            }
            for row in rows
        ]
