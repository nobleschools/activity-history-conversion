"""
test_activity_histories_upload.py
"""

from collections import OrderedDict
from unittest import mock

import pytest
#from simple_salesforce import Salesforce

from src.convert_activity_histories import _group_results_by_subject


NUMBER_OF_RECORDS = 5

ungrouped_result_dicts = [
    OrderedDict([
        ("Subject", "← Email: Recommendations"),
    ]),
    OrderedDict([
        ("Subject", "← Email: Scholarship question"),
    ]),
    OrderedDict([
        ("Subject", "← Email: Re: Recommendations"),
    ]),
]

@pytest.fixture()
def mock_connection():
    MockConnection = mock.create_autospec(Salesforce)

    first_results = {
        "totalSize": NUMBER_OF_RECORDS,
        "done": False,
        "nextRecordsUrl": "https://ne.xt",
        "records": [1, 2, 3],
    }
    last_results = {
        "totalSize": NUMBER_OF_RECORDS,
        "done": True,
        "nextRecordsUrl": "",
        "records": [4, 5],
    }

    # test the test
    assert NUMBER_OF_RECORDS == \
        len(first_results["records"]) + len(last_results["records"])

    MockConnection().query.return_value = first_results
    MockConnection().query_more.return_value = last_results

    return MockConnection()


class TestActivityHistoryUpload():

    @pytest.mark.paremetrize("results_list", ungrouped_result_dicts)
    def test_make_subject_groups(self):
        grouped_dicts = _group_results_by_subject(ungrouped_result_dicts)
        assert len(grouped_dicts) == 2

        for group in grouped_dicts:
            if len(group) == 1:
                assert "Scholarship" in group[0]["Subject"]
            elif len(group) == 2:
                assert "Recommendation" in group[0]["Subject"]
            else:
                pytest.fail("Response dicts not properly grouped by subject")

