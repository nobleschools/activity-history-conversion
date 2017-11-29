"""
activity_history_conversion/src/lambda_function.py

Lambda wrapper for convert_activity_histories. Creates Contact Notes from
Activity History and Event Salesforce objects.
"""

from base64 import urlsafe_b64decode
import os
import urllib

import boto3

from src import convert_activity_histories


# decrypt env vars once here so they're available to subsequent lambda
# calls on same container
env_vars = (
    "PAPERTRAIL_HOST",
    "PAPERTRAIL_PORT",
    "SF_LIVE_PASSWORD",
    "SF_LIVE_TOKEN",
)

for var in env_vars:
    encrypted = os.environ[var]
    decrypted = boto3.client("kms").decrypt(
        CiphertextBlob=urlsafe_b64decode(encrypted)
    )["Plaintext"]
    os.environ[var] = decrypted.decode()


def lambda_handler(event, context):
    """
    ...

    :param event: dict AWS event source dict
    :param context: LambdaContext object
    """
    pass


def push_to_s3(file_path, bucket_name):
    """Push the file at file_path to the s3 bucket_name.

    :param file_path: str path to file to be uploaded (should be in /tmp/)
    :param bucket_name: str name of bucket to push file to
    :return: None
    :rtype: None
    """
    file_name = file_path.split("/")[-1]
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)
    file_dest = os.path.join(REPORTS_DIR, file_name)
    bucket.upload_file(Key=file_dest, Filename=file_path)


if __name__ == "__main__":
    pass
