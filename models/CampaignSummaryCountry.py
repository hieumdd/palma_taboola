from models.models import Taboola
from components.getter import CampaignFilterGetter


class CampaignSummaryCountry(Taboola):
    table = "CampaignSummaryCountry"
    endpoint = "reports/campaign-summary/dimensions/country_breakdown"
    getter = CampaignFilterGetter
    keys = {
        "p_key": [
            "start_date",
            "end_date",
            "campaign",
            "country",
            "country_name"
        ],
        "incre_key": "last_used_rawdata_update_time",
    }
    schema = [
        {"name": "country", "type": "STRING"},
        {"name": "country_name", "type": "STRING"},
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
        {"name": "campaigns_num", "type": "INTEGER"},
        {"name": "cpa", "type": "FLOAT"},
        {"name": "cpa_actions_num", "type": "INTEGER"},
        {"name": "cpa_conversion_rate", "type": "FLOAT"},
        {"name": "currency", "type": "STRING"},
        {"name": "campaign", "type": "STRING"},
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
                "country": row["country"],
                "country_name": row["country_name"],
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
                "campaigns_num": row["campaigns_num"],
                "cpa": row["cpa"],
                "cpa_actions_num": row["cpa_actions_num"],
                "cpa_conversion_rate": row["cpa_conversion_rate"],
                "currency": row["currency"],
                "campaign": result["campaign"],
                "start_date": result["metadata"]["start_date"],
                "end_date": result["metadata"]["end_date"],
                "last_used_rawdata_update_time": result[
                    "last-used-rawdata-update-time"
                ],
            }
            for row in rows
        ]
