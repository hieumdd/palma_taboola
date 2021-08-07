from .utils import process


START = "2021-06-28"
END = "2021-08-06"


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


def test_campaign_summary_hourly():
    data = {
        "table": "CampaignSummaryHourly",
        "start": START,
        "end": END,
    }
    process(data)
