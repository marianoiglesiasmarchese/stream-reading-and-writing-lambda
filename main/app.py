import json
import os
import logging
import pandas as pd
import boto3
from smart_open import open


# setup logger
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO').upper())


def lambda_handler(event, context):
    # shared attributes
    s3_endpoint = os.environ.get('S3_ENDPOINT', 'http://s3.amazonaws.com/')
    s3_bucket = event.get("bucket")
    s3_key = event.get("key")

    """
    stream reading and writing with boto3
    """
    read_and_persistence_with_boto3(s3_endpoint, s3_bucket, s3_key)

    """
    stream reading and writing with smart_open
    """
    read_and_persistence_with_smart_open(s3_endpoint, s3_bucket, s3_key)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": event,
        }),
    }


def read_and_persistence_with_boto3(s3_endpoint, s3_bucket, s3_key):
    # boto3 s3 client
    s3_client = boto3.client('s3', endpoint_url=s3_endpoint)

    # fetching an object
    obj = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
    csv_file_df = pd.read_csv(obj['Body'])
    logger.info('file: \n %s', csv_file_df)

    # pushing it!
    s3_client.put_object(Body=csv_file_df.to_csv(index=False), Bucket=s3_bucket, Key='result/' + s3_key)
    logger.info("persistence on aws s3 done with boto3!")


def read_and_persistence_with_smart_open(s3_endpoint, s3_bucket, s3_key):
    # Stream from/to localstack bucket providing endpoint from boto3
    transport_params = {
        'session': boto3.Session(),
        'resource_kwargs': {
            'endpoint_url': s3_endpoint
        }
    }

    # fetching and object
    with open('s3://' + s3_bucket + '/' + s3_key, transport_params=transport_params) as file:
        csv_file_df = pd.read_csv(file)
        logger.info('file: \n %s', csv_file_df)

    # pushing it!
    with open('s3://test-bucket/key.txt', 'w', transport_params=transport_params) as fout:
        csv_file_df.to_csv(fout, index=False)
        logger.info("persistence on aws s3 done with smart_open!")