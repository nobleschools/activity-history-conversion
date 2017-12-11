"""
activity_history_conversion/src/convert_activity_history.py

Create Contact Notes from Activity History and Event Salesforce objects.

...
"""

import csv
from datetime import (
    datetime,
    timedelta,
)
from itertools import chain
from os import path
import re

from fuzzywuzzy import fuzz
import pytz

from salesforce_fields import activity_history as ah_fields
from salesforce_fields import contact_note as cn_fields
from salesforce_utils import (
    get_or_create_contact_note,
    get_salesforce_connection,
    make_salesforce_datestr,
    salesforce_gen,
)
from salesforce_utils.constants import (
        AC_LOOKUP,
        CAMPUS_SF_IDS,
        ROWECLARK,
        SALESFORCE_DATETIME_FORMAT,
)
from noble_logging_utils.papertrail_logger import (
    get_logger,
    SF_LOG_LIVE,
    SF_LOG_SANDBOX,
)



from pprint import pprint # XXX dev


DAYS_BACK = 1 # convert objects from last DAYS_BACK days

ROWECLARK_ACCOUNT_ID = CAMPUS_SF_IDS[ROWECLARK]
AC_ID = AC_LOOKUP["rc"][0]

SOURCE_DATESTR_FORMAT = "%m/%d/%Y"
OUTFILE_DATESTR_FORMAT = "%Y%m%d-%H:%M"

# simple_salesforce.Salesfoce.bulk operation result keys
SUCCESS = "success" # bool
ERRORS = "errors"   # list
ID_RESULT = "id"    # str safe id
CREATED = "created" # bool

# actions
CREATE = "create"

SUBJECT_MATCH_THRESHOLD = 100
ACS_TO_CONVERT = (
    "", # maluna
)

NEWLINE_RE = re.compile("^\s*\n+", re.MULTILINE)


def convert_ah_and_events_to_contact_notes(sandbox=False): # XXX dev
    """Look for recent Activity History and Event objects and make
    Contact Notes from them.

    ...

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

    ## convert_events()



def convert_activity_histories(sf_connection, start_date):
    """Make Contact Note objects from recent Activity History objects.

    Results must be sorted by WhoID then CreatedDate for object grouping later
    (grouping objects by contact/WhoId, with a similar subject with a
    shared CreatedDate).

    :param start_date: str earliest (created) date from which to convert
        objects, in SALESFORCE_DATETIME_FORMAT (%Y-%m-%dT%H:%M:%S.%f%z)
    :return: ???
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
            f"AND {ah_fields.CREATED_DATE} >= {start_date} "
            #f"AND {ah_fields.CREATED_DATE} >= 2013-01-01T00:00:00+0000 " # XXX dev
            #f"AND {ah_fields.CREATED_DATE} < 2014-01-01T00:00:00+0000 " # XXX dev
            f"ORDER BY {ah_fields.WHO_ID}, {ah_fields.CREATED_DATE} ASC "
        f") "
        f"FROM Account WHERE Id = '{ROWECLARK_ACCOUNT_ID}' "
    )
    # lookup query results are nested..
    ah_results = next(salesforce_gen(sf_connection, ah_query))
    if not ah_results["ActivityHistories"]:
        logger.info(f"No ActivityHistory objects created after {start_date}")
        return

    records = ah_results["ActivityHistories"]["records"]
    resulting_notes = []
    ah_ids = []
    # group down by alum contact, then date
    grouped_by_whoid = _group_records(records, lambda x: x[ah_fields.WHO_ID])
    for whoid_group in grouped_by_whoid:
        grouped_by_created_date = _group_records(
            whoid_group, lambda x: x[ah_fields.CREATED_DATE][:10]
        )
        for created_date_group in grouped_by_created_date:
            grouped_by_subject = _group_records_by_subject(created_date_group)
            for subject_group in grouped_by_subject:
                if not subject_group: # TODO handle upstream
                    continue
                # assuming longest email in a group with matching Subject
                # fields contains whole of transaction
                # from that day, upload as Contact Note
                longest = max(
                    subject_group, key=lambda x: len(x[ah_fields.DESCRIPTION])
                )
                ah_ids.append({"Id": longest[ah_fields.ID]})
                prepped = _map_ah_to_contact_note(longest)
                #pprint(prepped)
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
    #description = ah_record_dict[ah_fields.DESCRIPTION][:100] # XXX dev
    # strip out instances of >2 '\n' in a row for nicer formatting
    description = NEWLINE_RE.sub(
        "\n", ah_record_dict[ah_fields.DESCRIPTION]
    )
    # escape apostrophes for SF query
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


def convert_events():
    """Make Contact Note objects from recent Event objects.

    ...

    """
    pass
    events_query = (
        f"SELECT {event_fields.ID} "
        f",{event_fields.OWNER_ID} " # Rowe-Clark Coordinator; Event.Assigned To
        f",{event_fields.WHO_ID} " # Contact (alum) SFID; Event.Name; --> Contact__c
        f",{event_fields.SUBJECT} " # --> Subject__c
        f",{event_fields.DESCRIPTION} " # --> Comments__c
        f",{event_fields.START_DATETIME} " # --> Date_of_Contact__c
        f"FROM {event_fields.API_NAME} "
        f"WHERE OwnerId = '{AC_ID}' " # Rowe-Clark Coordinator
    )


    # prep_for_sf ... description could be None




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



def upload_contact_notes(infile_path, output_dir, sandbox=False):
    """Creates Contact Notes from Comer where necessary, and saves a report
    file matching Noble's Contact Note SF ID to Comer's.

    Checks if a note already exists at Noble, and if so, adds the existing
    Noble SF ID to the report, for Comer to sync in their SF instance.

    :pararm infile_path: str path to the input file
    :param output_dir: str path to the directory in which output file is saved
    :param sandbox: (optional) bool whether or not to use the sandbox
        Salesforce instance
    :return: path to the created csv report file
    :rtype: str
    """
    global sf_connection
    sf_connection = get_salesforce_connection(sandbox=sandbox)
    job_name = __file__.split(path.sep)[-1]
    hostname = SF_LOG_SANDBOX if sandbox else SF_LOG_LIVE
    global logger
    logger = get_logger(job_name, hostname=hostname)

    for_bulk_create = []

    with open(infile_path, "r") as infile:
        reader = csv.DictReader(infile)

        for row in reader:
            # standard with exported Salesforce reports, expecting a blank row
            # after the data before footer metadata rows
            if _is_blank_row(row):
                break

            # likely means it's a GCYC alum
            if not row[NOBLE_CONTACT_SF_ID]:
                continue

            # utmostu grade point calculator notes uploaded without a date of
            # contact; skip
            if not row[DATE_OF_CONTACT]:
                continue

            if not row[NOBLE_CONTACT_NOTE_SF_ID]:
                for_bulk_create.append(row)

    created_results = _create_contact_notes(for_bulk_create)
    report_path = _save_created_report(
        created_results, output_dir, for_bulk_create
    )

    return report_path


def _save_created_report(results_list, output_dir, args_dicts):
    """Save a csv report of newly-created Contact Notes to send to Comer.

    Also saves reference to found duplicates, in case the reference isn't
    present in Comer's Salesforce.

    :param results_list: list of result dicts
    :param output_dir: str path to directory where to save report file
    :param args_dicts: iterable of original args_dicts, to pull college SF ID
    :return: None
    """
    now_datetime = datetime.now().strftime(OUTFILE_DATESTR_FORMAT)
    filename = f"New_Noble_Contact_Notes_{now_datetime}.csv"
    file_path = path.join(output_dir, filename)

    report_headers = (
        NOBLE_CONTACT_NOTE_SF_ID,
        COMER_CONTACT_NOTE_SF_ID,
    )

    if results_list:
        headers = report_headers
        with open(file_path, "w") as fhand:
            writer = csv.DictWriter(
                fhand, fieldnames=headers, extrasaction="ignore"
            )
            writer.writeheader()
            for result, args_dict in zip(results_list, args_dicts):
                # mapping back from Salesforce to source headers for Comer
                result[NOBLE_CONTACT_NOTE_SF_ID] = result[ID_RESULT]
                writer.writerow(result)
    else:
        with open(file_path, "w") as fhand:
            writer = csv.Writer(fhand)
            writer.writerow("No new Noble Contact Note objects saved.")

    logger.info(f"Saved new Noble Contact Notes report to {file_path}")

    return file_path


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
                ID_RESULT: result[ID_RESULT],
                ERRORS: result[ERRORS],
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


def _prep_row_for_salesforce(row_dict):
    """Change keys in row_dict to Salesforce API field names, and convert
    data where necessary.

    Changes keys in the row_dict to Salesforce API fieldnames, filtering out
    irrelevant keys (ie. those outside of the FIELD_CONVERSIONS lookup).
    After converting the keys, prepares row for simple_salesforce api:
        - converts source datetime str to Salesforce-ready
        - converts checkbox bools to python bools
        - insert a default Subject where source is blank

    :param row_dict: dict row of Contact Note data
    :return: new dict ready for Salesforce bulk action
    :rtype: dict
    """
    # maps input headers to Salesforce field API names
    FIELD_CONVERSIONS = {
        NOBLE_CONTACT_SF_ID: contact_note_fields.CONTACT,
        COMMENTS: contact_note_fields.COMMENTS,
        COMM_STATUS: contact_note_fields.COMMUNICATION_STATUS,
        DATE_OF_CONTACT: contact_note_fields.DATE_OF_CONTACT,
        DISCUSSION_CATEGORY: contact_note_fields.DISCUSSION_CATEGORY,
        INITIATED_BY_ALUM: contact_note_fields.INITIATED_BY_ALUM,
        MODE: contact_note_fields.MODE_OF_COMMUNICATION,
        SUBJECT: contact_note_fields.SUBJECT,
    }

    new_dict = dict()
    for source_header, api_name in FIELD_CONVERSIONS.items():
        datum = row_dict.get(source_header, None)
        if datum:
            new_dict[api_name] = datum

    # Subject is required
    DEFAULT_SUBJECT = "(note from spreadsheet)"
    source_subject = new_dict.get(contact_note_fields.SUBJECT, None)
    if not source_subject:
        new_dict[contact_note_fields.SUBJECT] = DEFAULT_SUBJECT

    # convert DATE_OF_CONTACT to salesforce-ready datestr
    source_datestr = new_dict.get(contact_note_fields.DATE_OF_CONTACT, None)
    if source_datestr:
        salesforce_datestr = make_salesforce_datestr(
            source_datestr, SOURCE_DATESTR_FORMAT
        )
        new_dict[contact_note_fields.DATE_OF_CONTACT] = salesforce_datestr

    # INITIATED_BY_ALUM comes as str '1' or '0' (checkbox in Salesforce);
    # convert to explicit bool for simple_salesforce api
    source_initiated = new_dict.get(contact_note_fields.INITIATED_BY_ALUM, '0')
    initiated_bool = bool(int(source_initiated))
    new_dict[contact_note_fields.INITIATED_BY_ALUM] = initiated_bool

    return new_dict


def _is_blank_row(row_dict):
    """Checks if row is blank, signaling end of data in spreadsheet.

    Reports from Salesforce are generated with footer rows at the end,
    separated from the actual report data by one blank row.
    """
    return all(v == "" for v in row_dict.values())


if __name__ == "__main__":
    pass
