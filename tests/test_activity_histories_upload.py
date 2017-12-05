"""
test_activity_histories_upload.py
"""

from collections import OrderedDict
from unittest import mock

import pytest
#from simple_salesforce import Salesforce

from convert_activity_histories import (
    _group_records,
    _group_records_by_subject,
)
from salesforce_fields import activity_history as ah_fields


NUMBER_OF_RECORDS = 5

# OrderedDicts are expected type
ungrouped_record_dicts = [
    OrderedDict([
        ("Subject", "← Email: Recommendations"),
        ("WhoId", "abc123"),
        ("ActivityDate", "2017-12-01"),
    ]),
    OrderedDict([
        ("Subject", "← Email: Scholarship question"),
        ("WhoId", "def456"),
        ("ActivityDate", "2017-11-01"),
    ]),
    OrderedDict([
        ("Subject", "← Email: Re: Recommendations"),
        ("WhoId", "abc123"),
        ("ActivityDate", "2017-12-01"),
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

    @pytest.mark.parametrize("records_list", [ungrouped_record_dicts])
    def test_make_subject_groups(self, records_list):
        grouped_dicts = _group_records_by_subject(records_list)
        assert len(grouped_dicts) == 2

        for group in grouped_dicts:
            if len(group) == 1:
                assert "Scholarship" in group[0]["Subject"]
            elif len(group) == 2:
                assert "Recommendation" in group[0]["Subject"]
            else:
                pytest.fail("Response dicts not properly grouped by subject")


    @pytest.mark.parametrize("records_list", [ungrouped_record_dicts])
    def test_group_records_by_whoid(self, records_list):
        # assumes they'll be sorted
        key = ah_fields.WHO_ID
        sorted_ = sorted(records_list, key=lambda x: x[key])
        grouped_dicts = _group_records(sorted_, key)
        assert len(grouped_dicts) == 2

        for group in grouped_dicts:
            if len(group) == 1:
                assert group[0][key] == "def456"
            elif len(group) == 2:
                assert group[0][key] == "abc123"
            else:
                pytest.fail("Response dicts not properly grouped by WhoId")


    @pytest.mark.parametrize("records_list", [ungrouped_record_dicts])
    def test_group_records_by_activity_date(self, records_list):
        # assumes they'll be sorted
        key = ah_fields.ACTIVITY_DATE
        sorted_ = sorted(records_list, key=lambda x: x[key])
        grouped_dicts = _group_records(sorted_, key)
        assert len(grouped_dicts) == 2

        for group in grouped_dicts:
            if len(group) == 1:
                assert group[0][key] == "2017-11-01"
            elif len(group) == 2:
                assert group[0][key] == "2017-12-01"
            else:
                pytest.fail("Response dicts not properly grouped by ActivityDate")
