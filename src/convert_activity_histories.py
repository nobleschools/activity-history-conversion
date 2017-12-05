"""
activity_history_conversion/src/convert_activity_history.py

Create Contact Notes from Activity History and Event Salesforce objects.

...
"""

import csv
from datetime import datetime
from itertools import chain
from os import path

from fuzzywuzzy import fuzz

from salesforce_fields import activity_history as ah_fields
from salesforce_fields import contact_note as contact_note_fields
from salesforce_fields import contact as contact_fields
from salesforce_utils import (
    get_salesforce_connection,
    make_salesforce_datestr,
    salesforce_gen,
)
from salesforce_utils.constants import (
        AC_LOOKUP,
        CAMPUS_SF_IDS,
        ROWECLARK,
)
from noble_logging_utils.papertrail_logger import (
    get_logger,
    SF_LOG_LIVE,
    SF_LOG_SANDBOX,
)

ROWECLARK_ACCOUNT_ID = CAMPUS_SF_IDS[ROWECLARK]
AC_ID = AC_LOOKUP["rc"]

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

def convert_ah_and_events_to_contact_notes(start_date, sandbox=True): # XXX dev
    """Look for recent Activity History and Event objects and make
    Contact Notes from them.

    ...

    """
    global sf_connection
    sf_connection = get_salesforce_connection(sandbox=sandbox)
    global logger
    job_name = __file__.split(path.sep)[-1]
    hostname = SF_LOG_SANDBOX if sandbox else SF_LOG_LIVE
    logger = get_logger(job_name, hostname=hostname)

    ## convert_activity_histories()

    ## convert_events()

    ## ..logging..


def convert_activity_histories(start_date, sf_connection):
    """Make Contact Note objects from recent Activity History objects.

    Results must be sorted by WhoID then ActivityDate for object grouping later
    (grouping objects by contact/WhoId, with a similar subject with a
    shared ActivityDate).

    :param start_date: str earliest (activity) date from which to convert
        objects, in SALESFORCE_DATESTRING_FORMAT (%Y-%m-%d)
    :return: ???
    """
    ah_query = (f"""
        SELECT (
            SELECT {ah_fields.ID}
            ,{ah_fields.SUBJECT}
            ,{ah_fields.CREATED_DATE}
            ,{ah_fields.WHO_ID}
            ,{ah_fields.ACTIVITY_DATE}
            FROM {ah_fields.API_NAME}
            WHERE IsTask = True
            AND OwnerId = '{AC_ID}'
            AND {ah_fields.ACTIVITY_DATE} >= '{start_date}'
            ORDER BY {ah_fields.WHO_ID}, {ah_fields.ACTIVITY_DATE} ASC
        )
        FROM Account WHERE Id = '{ROWECLARK_ACCOUNT_ID}'
    """)
    # lookup query results are nested..
    ah_results = next(salesforce_gen(sf_connection, ah_query))
    records = ah_results["ActivityHistories"]["records"]

    # pare down by date by subject
    grouped_by_whoid = _group_records(records, ah_fields.WHO_ID)
    for whoid_group in grouped_by_whoid:
        grouped_by_activity_date = _group_records(
            whoid_group, ah_fields.ACTIVITY_DATE
        )
        for activity_date_group in grouped_by_activity_date:
            grouped_by_subject = _group_records_by_subject(activity_date_group)
            for final_group in grouped_by_subject:
                longest = max(
                    final_group, key=lambda x: len(x[ah_fields.DESCRIPTION])
                )


    resulting_notes = []
    ah_ids = []
    for ah_dict in pared_results:
        ah_ids.append(ah_dict[ah_fields.ID])
        prepped = _map_ah_to_contact_note(ah_dict)
        #contact_note = get_or_create_contact_note(prepped)
        #resulting_notes.append(contact_note)

    _log_results("Activity Histories", resulting_notes, ah_ids)


def _map_ah_to_contact_note(result_dict):
    """From a dict of Activity History data, create a dict of args for a
    (new) Contact Note.

    :param result_dict: dict of Activity History data, expecting the below
        key names and value types:
            ah_fields.ID: str
            ah_fields.WHO_ID: str
            ah_fields.SUBJECT: str
            ah_fields.DESCRIPTION: str
            ah_fields.CREATED_DATE: str
    :return: dict of Contact Note data, keyed by Salesforce API names
    :rtype: dict
    """

    # Mode_of_Communication__c = "Email"
    # Contact__c --> whoID
    # Subject__c --> Subject !!! could be None
    # Comments__c --> description (rolled up)
    # Date_of_Communication --> CreatedDate?


def convert_events():
    """Make Contact Note objects from recent Event objects.

    ...

    """
    pass


def _group_records(record_dicts, key):
    """Group the record dicts by the value of the passed key arg, so that each
    sub-list returned is a list of dicts with the same value.

    :param record_dicts: list of ``simple_salesforce.Salesforce.query`` result
        record dicts
    :param key: str key by which to group. Should be one of
        ah_fields.WHO_ID
        ah_fields.ACTIVITY_DATE
    :return: list of lists, where the dicts in each sub-list share the same key
    :rtype: list
    """
    grouped = []

    current_value = None
    current_value_group = []
    for record in record_dicts:
        value = record[key]
        if not current_value:
            current_value = value
        elif value is not current_value:
            grouped.append(current_value_group)
            current_value = value
            current_value_group = []

        current_value_group.append(record)
    grouped.append(current_value_group)

    return grouped


def _group_records_by_subject(records_list):
    """Group the record dicts by related (email) Subject.

    Eg. should group together emails with subjects of "Recommendation" and
    "re: Recommendation", in a separate group from email with subject
    "School visit".

    :param records_list: list of ``simple_salesforce.Salesforce.query``
        result dicts, assumed to be related to the same Contact and from the
        same time period (eg. ActivityDate)
    :return: list of lists, where each sub-list contains record dicts with
        like subjects
    :rtype: list
    """
    all_grouped = []
    with_seen_flag = [[d, 0] for d in records_list]
    for result_pair in with_seen_flag:
        group = []
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
                group.append(other_result[0])
                other_result[1] = 1
        result_pair[1] = 1
        all_grouped.append(group)

    return all_grouped



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


def _create_contact_notes(data_dicts):
    """Create new Contact Notes after converting row to Salesforce-ready data
    dicts, and add Comer's 'Contact Note: ID' to the results.

    First checks for existing Notes with the same Contact (alum), Date of
    Contact, and Subject, and skip if one is found.

    The returned results format should otherwise mimic the format returned by
    ``simple-salesforce.Salesforce.bulk`` operations; ie. the following keys:
        - 'id'
        - 'success'
        - 'errors'
        - 'created'
        - COMER_CONTACT_NOTE_SF_ID

    :param data_dicts: iterable of dictionaries to create
    :return: list of results dicts
    :rtype: list
    """
    results = []
    for contact_note_dict in data_dicts:
        comer_id = contact_note_dict[COMER_CONTACT_NOTE_SF_ID]
        salesforce_ready = _prep_row_for_salesforce(contact_note_dict)
        result = get_or_create_note(salesforce_ready)
        # not added by non-bulk `create` call
        if result[SUCCESS]:
            result[CREATED] = True
        else:
            result[CREATED] = False
        result[COMER_CONTACT_NOTE_SF_ID] = comer_id
        results.append(result)

    _log_results(results, CREATE, data_dicts)
    return results


def get_or_create_note(contact_note_dict):
    """Look for an existing Contact Note with the same Contact, Subject, and
    Date of Contact fields. Return that if exists, otherwise create.

    :param contact_note_dict: dictionary of Contact Note details, with keys
        already mapped to Salesforce API fieldnames and dates API-ready
    :return: results dict (keys 'id', 'success', 'errors')
    :rtype: dict
    """
    alum_sf_id = contact_note_dict[contact_note_fields.CONTACT]
    subject = contact_note_dict[contact_note_fields.SUBJECT]
    date_of_contact = contact_note_dict[contact_note_fields.DATE_OF_CONTACT]
    contact_note_query = (
        f"SELECT {contact_note_fields.ID} "
        f"FROM {contact_note_fields.API_NAME} "
        f"WHERE {contact_note_fields.CONTACT} = '{alum_sf_id}' "
        f"AND {contact_note_fields.SUBJECT} = '{subject}' "
        f"AND {contact_note_fields.DATE_OF_CONTACT} = {date_of_contact} "
    )

    results = sf_connection.query(contact_note_query)
    if results["totalSize"]:
        # doesn't matter if more than one
        existing_sf_id = results["records"][0]["Id"]
        return {
            ID_RESULT: existing_sf_id,
            SUCCESS: False,
            ERRORS: [f"Found conflicting Contact Note {existing_sf_id}",],
        }

    return sf_connection.Contact_Note__c.create(contact_note_dict)


def _log_results(results_list, action, original_data):
    """Log results from Contact Note create action.

    Log results from create_contact_notes. Input results_list structured as
    if it were a ``simple_salesforce.Salesforce.bulk`` call for compatability
    with bulk updates and deletes. Expects the following keys in
    results_list dicts:
        - success
        - id
        - created
        - errors

    :param results_list: list of result dicts, mimicking
        ``simple_salesfoce.Salesforce.bulk`` result
    :param action: str action taken ('create', 'update', 'delete')
    :param original_data: list of original data dicts from the input file
    :rtype: None
    """
    logger.info(f"Logging results of bulk Contact Note {action} operation..")
    attempted = success_count = fail_count = 0
    for result, args_dict in zip(results_list, original_data):
        attempted += 1
        if not result[SUCCESS]:
            fail_count += 1
            log_payload = {
                "action": action,
                ID_RESULT: result[ID_RESULT],
                ERRORS: result[ERRORS],
                "arguments": args_dict,
            }
            logger.warn(f"Possible duplicate Contact Note: {log_payload}")
        else:
            success_count += 1
            logger.info(f"Successful Contact Note {action}: {result['id']}")

    logger.info(
        f"Contact Note {action}: {attempted} attempted, "
        f"{success_count} succeeded, {fail_count} failed."
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
