from models.models import Taboola
from components.getter import CampaignFilterGetter
from components.transformer import CampaignFilterTransformer


class CampaignSummaryOSVersion(Taboola):
    endpoint = "reports/campaign-summary/dimensions/os_version_breakdown"
    getter = CampaignFilterGetter
    transformer = CampaignFilterTransformer
    keys = {
        "p_key": [
            "start_date",
            "end_date",
            "campaign",
            "platform",
            "platform_name",
            "os_version"
        ],
        "incre_key": "last_used_rawdata_update_time",
    }
    schema = [
        {"name": "platform", "type": "STRING"},
        {"name": "platform_name", "type": "STRING"},
        {"name": "os_version", "type": "STRING"},
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
