"""
convert_activity_histories/cli.py

CLI for convert_activity_histories.py, creating Contact Notes from recent
Activity History and Event objects
"""

import argparse

from src import convert_ah_and_events_to_contact_notes


def main(sandbox=False):
    """
    """
    convert_ah_and_events_to_contact_notes(sandbox=sandbox)
    #print(f"Details on new Contact Notes saved to {new_noble_contact_notes}")


def parse_args():
    """
    """
    parser = argparse.ArgumentParser(description=\
        "Create Contact Notes from Activity History and Event objects"
    )
    parser.add_argument(
        "--sandbox", "-s",
        action="store_true",
        default=False,
        help="If passed, uses the sandbox Salesforce instance. Defaults to live",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(sandbox=args.sandbox)
