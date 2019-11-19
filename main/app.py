import json
import os
import logging
import pandas as pd
import boto3


# setup logger
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO').upper())


def lambda_handler(event, context):
    """
    stream reading and writing
    """
    read_and_persistence_with_boto3(event)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": event,
        }),
    }


def read_and_persistence_with_boto3(event):
    # boto3 s3 client
    s3_endpoint = os.environ.get('S3_ENDPOINT', 'http://s3.amazonaws.com/')
    s3_bucket = event.get("bucket")
    s3_key = event.get("key")
    s3_client = boto3.client('s3', endpoint_url=s3_endpoint)

    # fetching an object
    obj = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
    csv_file_df = pd.read_csv(obj['Body'])
    logger.info('file: \n %s', csv_file_df)

    # pushing it!
    s3_client.put_object(Body=csv_file_df.to_csv(index=False), Bucket=s3_bucket, Key='result/' + s3_key)
    logger.info("persistence on aws s3 done with boto3!")