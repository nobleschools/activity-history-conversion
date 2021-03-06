"""
activity_history_conversion/src/lambda_function.py

Lambda wrapper for convert_ah_and_events_to_contact_notes.
Creates Contact Notes from Activity History and Event Salesforce objects.
"""

from base64 import urlsafe_b64decode
import os

import boto3
import rollbar

from src import convert_ah_and_events_to_contact_notes


# decrypt env vars once here so they're available to subsequent lambda
# calls on same container
env_vars = (
    "PAPERTRAIL_HOST",
    "PAPERTRAIL_PORT",
    "SF_LIVE_PASSWORD",
    "SF_LIVE_TOKEN",
    "ROLLBAR_TOKEN",
)

for var in env_vars:
    encrypted = os.environ[var]
    decrypted = boto3.client("kms").decrypt(
        CiphertextBlob=urlsafe_b64decode(encrypted)
    )["Plaintext"]
    os.environ[var] = decrypted.decode()

rollbar.init(os.environ["ROLLBAR_TOKEN"], "production")


@rollbar.lambda_function
def lambda_handler(event, context):
    """Call Activity History and Event to Contact Note job.

    :param event: dict AWS event source dict
    :param context: LambdaContext object
    """
    convert_ah_and_events_to_contact_notes()


if __name__ == "__main__":
    pass
