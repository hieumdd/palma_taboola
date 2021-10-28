import pytest

from unittest.mock import Mock

from main import main


@pytest.mark.parametrize(
    "table",
    [
        "TopCampaignContent",
        "CampaignSummary",
        "CampaignSummaryHourly",
        "CampaignSummaryCountry",
        "CampaignSummaryBrowser",
        "CampaignSummaryOSFamily",
        "CampaignSummaryOSVersion",
        "CampaignSummaryRegion",
    ],
)
@pytest.mark.parametrize(
    (
        "start",
        "end",
    ),
    [
        (None, None),
        ("2021-10-10", "2021-10-28"),
    ],
    ids=[
        "auto",
        "manual",
    ],
)
def test_pipelines(table, start, end):
    data = {
        "table": table,
        "start": start,
        "end": end,
    }
    req = Mock(get_json=Mock(return_value=data), args=data)
    res = main(req)
    assert res["num_processed"] == res["output_rows"]
