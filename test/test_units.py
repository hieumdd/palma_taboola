import pytest

from unittest.mock import Mock

from main import main


@pytest.mark.parametrize(
    "table",
    [
        # "TopCampaignContent",
        # "CampaignSummary",
        # "CampaignSummaryHourly",
        "CampaignSummaryCountry",
        "CampaignSummaryOSFamily",
        "CampaignSummaryBrowser",
    ],
)
@pytest.mark.parametrize(
    (
        "start",
        "end",
    ),
    [
        (None, None),
        # ("2021-08-20", "2021-09-20"),
    ],
    ids=[
        "auto",
        # "manual",
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
