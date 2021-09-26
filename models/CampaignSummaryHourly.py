from models.models import Taboola
from components.getter import OneDayGetter
from components.transformer import SingleDayTransformer


class CampaignSummaryHourly(Taboola):
    endpoint = "reports/campaign-summary/dimensions/campaign_hour_breakdown"
    getter = OneDayGetter
    transformer = SingleDayTransformer
    keys = {
        "p_key": [
            "campaign",
            "campaign_name",
            "date",
        ],
        "incre_key": "last_used_rawdata_update_time",
    }
    schema = [
        {"name": "date", "type": "TIMESTAMP"},
        {"name": "campaign_name", "type": "STRING"},
        {"name": "campaign", "type": "STRING"},
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
        {"name": "start_date", "type": "TIMESTAMP"},
        {"name": "end_date", "type": "TIMESTAMP"},
        {"name": "last_used_rawdata_update_time", "type": "TIMESTAMP"},
    ]
