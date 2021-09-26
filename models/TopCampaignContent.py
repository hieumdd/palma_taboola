from models.models import Taboola
from components.getter import FullDayGetter
from components.transformer import SingleDayTransformer


class TopCampaignContent(Taboola):
    endpoint = "reports/top-campaign-content/dimensions/item_breakdown"
    getter = FullDayGetter
    transformer = SingleDayTransformer
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
