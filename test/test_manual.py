from .utils import process


START = "2021-06-01"
END = "2021-07-01"


def test_top_campaign_content():
    data = {
        "table": "TopCampaignContent",
        "start": START,
        "end": END,
    }
    process(data)


def test_campaign_summary():
    data = {
        "table": "CampaignSummary",
        "start": START,
        "end": END,
    }
    process(data)
