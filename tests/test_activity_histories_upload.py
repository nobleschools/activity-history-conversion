"""
test_activity_histories_upload.py
"""

from collections import OrderedDict
from unittest.mock import (
    MagicMock,
    Mock,
)

import pytest
from simple_salesforce import Salesforce

from convert_activity_histories import (
    convert_activity_histories,
    convert_events,
    _group_records,
    _group_records_by_subject,
)
from salesforce_fields import activity_history as ah_fields


START_DATE_FOR_TEST = "2017-12-07T00:00:00+0000"

# returned type OrderedDicts; nested to match simple_salesforce.query result
ungrouped_record_dicts = [
    OrderedDict([
        ("Subject", "← Email: Scholarship question"),
        ("Description", "nineteen characters"),
        ("WhoId", "def456"),
        ("CreatedDate", "2017-12-02"),
    ]),
    OrderedDict([
        ("Subject", "← Email: Recommendations"),
        ("Description", "shorter description"),
        ("WhoId", "abc123"),
        ("CreatedDate", "2017-12-05"),
    ]),
    OrderedDict([
        ("Subject", "← Email: Re: Recommendations"),
        ("Description",
         "the longer of the two descriptions\n\n\nwith matching subjects"),
        ("WhoId", "abc123"),
        ("CreatedDate", "2017-12-05"),
    ]),
]
salesforce_ah_results = OrderedDict([
    ("totalSize", 3),
    ("done", True),
    ("records", [
        OrderedDict([
            ("ActivityHistories",
                OrderedDict([
                    ("records", ungrouped_record_dicts),
                ]),
            ),
        ]),
    ]),
])


#no_dupe_found = {
#    "totalSize": 0,
#    "done": True,
#    "records": [],
#}
created_result = {
    "id": "new567",
    "success": True,
    "errors": [],
}

MockConnection = MagicMock(spec=Salesforce)
MockConnection.Contact_Note__c = Mock()

@pytest.fixture()
def mock_sf_connection_for_ah():

    MockConnection.query = MagicMock(return_value=salesforce_ah_results)
    MockConnection.Contact_Note__c.create = MagicMock(
        return_value=created_result
    )
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
        key_func = lambda x: x[ah_fields.WHO_ID]
        # _group_records expects they'll be sorted by key in question
        sorted_ = sorted(records_list, key=lambda x: x[ah_fields.WHO_ID])

        grouped_dicts = _group_records(sorted_, key_func)
        assert len(grouped_dicts) == 2

        for group in grouped_dicts:
            if len(group) == 1:
                assert key_func(group[0]) == "def456"
            elif len(group) == 2:
                assert key_func(group[0]) == "abc123"
            else:
                pytest.fail("Response dicts not properly grouped by WhoId")


    @pytest.mark.parametrize("records_list", [ungrouped_record_dicts])
    def test_group_records_by_created_date(self, records_list):
        key_func = lambda x: x[ah_fields.CREATED_DATE][:10]
        # _group_records expects they'll be sorted by key in question
        sorted_ = sorted(records_list, key=lambda x: x[ah_fields.CREATED_DATE])

        grouped_dicts = _group_records(sorted_, key_func)
        assert len(grouped_dicts) == 2

        for group in grouped_dicts:
            if len(group) == 1:
                assert key_func(group[0]) == "2017-12-02"
            elif len(group) == 2:
                assert key_func(group[0]) == "2017-12-05"
            else:
                pytest.fail("Response dicts not properly grouped by CreatedDate")


    def test_convert_activity_histories(self, mock_sf_connection_for_ah):
        convert_activity_histories(
            mock_sf_connection_for_ah, START_DATE_FOR_TEST
        )
        mock_sf_connection_for_ah.query.assert_called()
        mock_sf_connection_for_ah.Contact_Note__c.assert_any_call({
            cn_fields.MODE_OF_COMMUNICATION: "Email",
            cn_fields.CONTACT: "abc123",
            cn_fields.SUBJECT: "← Email: Re: Recommendations",
            cn_fields.DATE_OF_CONTACT: "2017-12-05",
            cn_fields.COMMENTS:\
                "the longer of the two descriptions\nwith matching subjects",
        })
        mock_sf_connection_for_ah.Contact_Note__c.assert_any_call({
            cn_fields.MODE_OF_COMMUNICATION: "Email",
            cn_fields.CONTACT: "def456",
            cn_fields.SUBJECT: "← Email: Scholarship question",
            cn_fields.DATE_OF_CONTACT: "2017-12-02",
            cn_fields.COMMENTS: "shorter description",
        })


    def test_convert_events(self):
        assert 0
