from .utils import process


def test_top_campaign_content():
    data = {
        "table": "TopCampaignContent",
    }
    process(data)


def test_campaign_summary():
    data = {
        "table": "CampaignSummary",
    }
    process(data)

def test_campaign_summary_hourly():
    data = {
        "table": "CampaignSummaryHourly",
    }
    process(data)
