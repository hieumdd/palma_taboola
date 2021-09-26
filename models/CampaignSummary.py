from models.models import Taboola
from components.getter import MultiDayGetter
from components.transformer import MultiDayTransformer


class CampaignSummary(Taboola):
    table = "CampaignSummary"
    endpoint = "reports/campaign-summary/dimensions/campaign_site_day_breakdown"
    getter = MultiDayGetter
    transformer = MultiDayTransformer
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
