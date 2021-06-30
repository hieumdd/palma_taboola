from unittest.mock import Mock

from main import main


def test_auto():
    data = {}
    req = Mock(get_json=Mock(return_value=data), args=data)
    res = main(req)
    results = res["results"]
    assert results["num_processed"] >= 0
    if results["num_processed"] > 0:
        assert results["num_processed"] > 0
        assert results["num_processed"] == results["output_rows"]


def test_manual():
    data = {"start": "2021-06-01", "end": "2021-06-02"}
    req = Mock(get_json=Mock(return_value=data), args=data)
    res = main(req)
    results = res["results"]
    assert results["num_processed"] >= 0
    if results["num_processed"] > 0:
        assert results["num_processed"] > 0
        assert results["num_processed"] == results["output_rows"]
