from models.models import Taboola
from components.getter import CampaignFilterGetter


class CampaignSummaryCountry(Taboola):
    table = "CampaignSummaryCountry"
    endpoint = "reports/campaign-summary/dimensions/country_breakdown"
    getter = CampaignFilterGetter
    keys = {
        "p_key": [
            "campaign",
            "campaign_name",
            "date",
        ],
        "incre_key": "last_used_rawdata_update_time",
    }
    schema = []

    def _transform(self, results):
        pass
