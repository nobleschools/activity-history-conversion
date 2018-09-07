"""
activity_history_conversion/src/convert_activity_history.py

Create Contact Notes from Activity History and Event Salesforce objects.

...
"""

from datetime import (
    datetime,
    timedelta,
)
from os import path
import re

from fuzzywuzzy import fuzz
import pytz

from salesforce_fields import activity_history as ah_fields
from salesforce_fields import contact_note as cn_fields
from salesforce_fields import event as event_fields
from salesforce_utils import (
    get_or_create_contact_note,
    get_salesforce_connection,
    salesforce_gen,
)
from salesforce_utils.constants import (
        CAMPUS_SF_IDS,
        ROWECLARK,
        SALESFORCE_DATETIME_FORMAT,
)
from noble_logging_utils.papertrail_logger import (
    get_logger,
    SF_LOG_LIVE,
    SF_LOG_SANDBOX,
)


DAYS_BACK = 2 # convert objects from last DAYS_BACK days

ROWECLARK_ACCOUNT_ID = CAMPUS_SF_IDS[ROWECLARK]
AC_ID = "005E0000001e8pNIAQ" # 'rc' account alias

# simple_salesforce.Salesfoce.bulk operation result keys
SUCCESS = "success" # :bool
CREATED = "created" # :bool

SUBJECT_MATCH_THRESHOLD = 100

NEWLINE_RE = re.compile("^\s*\n+", re.MULTILINE)


def convert_ah_and_events_to_contact_notes(sandbox=False):
    """Look for recent Activity History and Event objects and make
    Contact Notes from them.

    Both conversion operations check for existing notes, and skip creation
    where potential duplicates are found.

    :param sandbox: bool if True, uses a connection to the configured
        sandbox Salesforce instance. Defaults to False
    :return: None
    :rtype: None
    """
    global sf_connection
    sf_connection = get_salesforce_connection(sandbox=sandbox)

    global logger
    job_name = __file__.split(path.sep)[-1]
    system_name = SF_LOG_SANDBOX if sandbox else SF_LOG_LIVE
    logger = get_logger(job_name, hostname=system_name)

    today = datetime.today()
    today_utc = today.astimezone(pytz.utc)
    start_date = today_utc - timedelta(days=DAYS_BACK)
    start_datestr = datetime.strftime(start_date, SALESFORCE_DATETIME_FORMAT)

    convert_activity_histories(sf_connection, start_datestr)
    convert_events(sf_connection, start_datestr)


def convert_activity_histories(sf_connection, start_date):
    """Make Contact Note objects from recent Activity History objects.

    Results must be sorted by WhoID then CreatedDate for object grouping later
    (grouping objects by contact/WhoId, with a similar subject with a
    shared CreatedDate).

    :param sf_connection: ``simple_salesforce.Salesforce`` connection
    :param start_date: str earliest (created) date from which to convert
        objects, in SALESFORCE_DATETIME_FORMAT (%Y-%m-%dT%H:%M:%S.%f%z)
    :return: None
    :rtype: None
    """
    ah_query = (
        f"SELECT ( "
            f"SELECT {ah_fields.ID} "
            f",{ah_fields.SUBJECT} "
            f",{ah_fields.CREATED_DATE} "
            f",{ah_fields.WHO_ID} "
            f",{ah_fields.DESCRIPTION} "
            f"FROM {ah_fields.API_NAME} "
            f"WHERE IsTask = True "
            f"AND {ah_fields.OWNER_ID} = '{AC_ID}' "
            f"AND {ah_fields.WHO_ID} != NULL "
            f"AND {ah_fields.CREATED_DATE} >= {start_date} "
            f"ORDER BY {ah_fields.WHO_ID}, {ah_fields.CREATED_DATE} ASC "
        f") "
        f"FROM Account WHERE Id = '{ROWECLARK_ACCOUNT_ID}' "
    )
    # lookup query results are nested..
    ah_results = next(salesforce_gen(sf_connection, ah_query))
    resulting_notes = []
    ah_ids = []

    if not ah_results["ActivityHistories"]:
        _log_results("Activity History", resulting_notes, ah_ids)
        return

    records = ah_results["ActivityHistories"]["records"]
    # group down by alum contact, then date
    grouped_by_whoid = _group_records(records, lambda x: x[ah_fields.WHO_ID])
    for whoid_group in grouped_by_whoid:
        grouped_by_created_date = _group_records(
            whoid_group, lambda x: x[ah_fields.CREATED_DATE][:10]
        )
        for created_date_group in grouped_by_created_date:
            grouped_by_subject = _group_records_by_subject(created_date_group)
            for subject_group in grouped_by_subject:
                if not subject_group: # TODO handle upstream (Desc != NULL?)
                    continue

                # where multiple matching Subjects from a given day and Contact,
                # assume the longest email contains all preceeding replies
                # in its body, and upload that as representative of the chain
                longest = max(
                    subject_group, key=lambda x: len(x[ah_fields.DESCRIPTION])
                )
                ah_ids.append({"Id": longest[ah_fields.ID]})
                prepped = _map_ah_to_contact_note(longest)
                result_dict = get_or_create_contact_note(sf_connection, prepped)
                if result_dict[SUCCESS]:
                    result_dict[CREATED] = True
                else:
                    result_dict[CREATED] = False
                resulting_notes.append(result_dict)

    _log_results("Activity History", resulting_notes, ah_ids)


def _map_ah_to_contact_note(ah_record_dict):
    """From a dict of Activity History data, create a dict of args for a
    (new) Contact Note.

    Does some cleaning of the data as well:
        - replace \n(\n)+ with \n in the Description/Comments__c field to
          cut out large swaths of empty space

    :param ah_record_dict: dict of Activity History data, expecting the below
        key names and value types:
            ah_fields.ID: str
            ah_fields.WHO_ID: str
            ah_fields.SUBJECT: str
            ah_fields.DESCRIPTION: str
            ah_fields.CREATED_DATE: str
    :return: dict of Contact Note data, keyed by Salesforce API names
    :rtype: dict
    """
    ah_id = ah_record_dict[ah_fields.ID]
    # strip out instances of >2 '\n' in a row for nicer formatting
    description = NEWLINE_RE.sub(
        "\n", ah_record_dict[ah_fields.DESCRIPTION]
    )
    cn_dict = {
        cn_fields.MODE_OF_COMMUNICATION: "Email",
        cn_fields.CONTACT: ah_record_dict[ah_fields.WHO_ID],
        cn_fields.SUBJECT: ah_record_dict[ah_fields.SUBJECT],
        # send YYYY-MM-DD
        cn_fields.DATE_OF_CONTACT: ah_record_dict[ah_fields.CREATED_DATE][:10],
        cn_fields.COMMENTS:\
            f"{description}\n\n///Created from ActivityHistory {ah_id}"
    }

    return cn_dict


def _map_event_to_contact_note(event_record_dict):
    """From a dict of Event data, create a dict of args for a (new)
    Contact Note.

    Does some cleaning of the data as well:
        - replace \n(\n)+ with \n in the Description/Comments__c field to
          cut out large swaths of empty space

    TODO: Roll together with other mapping function(s).

    :param event_record_dict: dict of Event data, expecting the below
        key names and value types:
            event_fields.ID: str
            event_fields.WHO_ID: str
            event_fields.SUBJECT: str
            event_fields.DESCRIPTION: str
            event_fields.START_DATETIME: str
    :return: dict of Contact Note data, keyed by Salesforce API names
    :rtype: dict
    """
    event_id = event_record_dict[event_fields.ID]
    # where empty Descriptions come back as None type, use string 'None'
    description = str(event_record_dict[event_fields.DESCRIPTION])
    # strip out instances of >2 '\n' in a row for nicer formatting
    description = NEWLINE_RE.sub("\n", description)
    cn_dict = {
        cn_fields.CONTACT: event_record_dict[event_fields.WHO_ID],
        cn_fields.SUBJECT: event_record_dict[event_fields.SUBJECT],
        # Contact Note just needs YYYY-MM-DD
        cn_fields.DATE_OF_CONTACT: event_record_dict[event_fields.START_DATETIME][:10],
        cn_fields.COMMENTS:\
            f"{description}\n\n///Created from Event {event_id}"
    }

    return cn_dict


def convert_events(sf_connection, start_datestr):
    """Make Contact Note objects from recent Event objects.

    Uses CREATED_DATE to pull recent Event objects, but Date of Contact
    set to the Event's StartDateTime. Checks for existing Contact Note, and
    won't create new if one is found.

    :param sf_connection: ``simple_salesforce.Salesforce`` connection
    :param start_date: str earliest (created) date from which to convert
        objects, in SALESFORCE_DATETIME_FORMAT (%Y-%m-%dT%H:%M:%S.%f%z)
    :return: None
    :rtype: None
    """
    events_query = (
        f"SELECT {event_fields.ID} "
        f",{event_fields.WHO_ID} " # --> Contact__c
        f",{event_fields.SUBJECT} " # --> Subject__c
        f",{event_fields.DESCRIPTION} " # --> Comments__c
        f",{event_fields.START_DATETIME} " # --> Date_of_Contact__c
        f"FROM {event_fields.API_NAME} "
        f"WHERE {event_fields.CREATED_DATE} >= {start_datestr} "
        f"AND {event_fields.WHO_ID} != NULL "
        f"AND OwnerId = '{AC_ID}' "
    )

    event_ids = []
    resulting_notes = []
    events = salesforce_gen(sf_connection, events_query)
    for event in events:
        event_ids.append({"Id": event[event_fields.ID]})
        prepped = _map_event_to_contact_note(event)
        result_dict = get_or_create_contact_note(sf_connection, prepped)
        # TODO roll into get_or_create
        if result_dict[SUCCESS]:
            result_dict[CREATED] = True
        else:
            result_dict[CREATED] = False
        resulting_notes.append(result_dict)

    _log_results("Event", resulting_notes, event_ids)


def _group_records(records_list, key_func):
    """Group the record dicts by the value of applying the passed key_func arg
    to each record in records_list.

    :param records_list: list of ``simple_salesforce.Salesforce.query`` result
        record dicts
    :param key_func: func key to get value by which to group
    :return: list of lists, where the dicts in each sub-list share the same
        key_func value
    :rtype: list
    """
    all_groups = []

    group_value = None
    sub_group = []
    for record in records_list:
        current_value = key_func(record)
        if not group_value:
            group_value = current_value
        elif current_value != group_value:
            all_groups.append(sub_group)
            group_value = current_value
            sub_group = []

        sub_group.append(record)
    all_groups.append(sub_group)

    return all_groups


def _group_records_by_subject(records_list):
    """Group the record dicts by related (email) Subject.

    Eg. should group together emails with subjects of "Recommendation" and
    "re: Recommendation", in a separate group from email with subject
    "School visit".

    :param records_list: list of ``simple_salesforce.Salesforce.query``
        result dicts, assumed to be related to the same Contact and from the
        same time period (eg. CreatedDate)
    :return: list of lists, where each sub-list contains record dicts with
        like subjects
    :rtype: list
    """
    all_groups = []
    with_seen_flag = [[d, 0] for d in records_list]
    for result_pair in with_seen_flag:
        sub_group = []
        if result_pair[1] == 1:
            continue
        target_subject = result_pair[0]["Subject"]
        for other_result in with_seen_flag:
            if other_result[1] == 1:
                continue
            match_score = fuzz.token_set_ratio(
                target_subject, other_result[0]["Subject"]
            )
            if match_score == SUBJECT_MATCH_THRESHOLD:
                sub_group.append(other_result[0])
                other_result[1] = 1
        result_pair[1] = 1
        all_groups.append(sub_group)

    return all_groups


def _log_results(original_object_name, results_list, original_data):
    """Log results from Contact Note create action.

    Log results from create_contact_notes. Input results_list structured as
    if it were a ``simple_salesforce.Salesforce.bulk`` call for compatability
    with bulk updates and deletes. Expects the following keys in
    results_list dicts:
        - success
        - id
        - created
        - errors

    :param original_object_name: str name of object type converted
    :param results_list: list of result dicts, mimicking
        ``simple_salesfoce.Salesforce.bulk`` result
    :param original_data: list of original data dicts from the input file
    :rtype: None
    """
    logger.info(
        f"Logging results of {original_object_name} to Contact Note conversion.."
    )
    attempted = success_count = fail_count = 0
    for result, args_dict in zip(results_list, original_data):
        attempted += 1
        if not result[SUCCESS]:
            fail_count += 1
            log_payload = {
                "from_object": original_object_name,
                "id": result["id"],
                "errors": result["errors"],
                "arguments": args_dict,
            }
            logger.warn(f"Possible duplicate Contact Note: {log_payload}")
        else:
            success_count += 1
            logger.info(
                f"Contact Note {result['id']} created from "
                f"{original_object_name} {args_dict['Id']}"
            )

    logger.info(
        f"{original_object_name} to Contact Note conversion: "
        f"{attempted} attempted, {success_count} succeeded, "
        f"{fail_count} failed."
    )


if __name__ == "__main__":
    pass
