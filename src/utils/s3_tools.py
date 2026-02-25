import os
from typing import Union

import boto3
import botocore
import requests
from tqdm import tqdm


def s3_url_to_bucket_and_key(url:str) -> (str,str):
    # eg: # s3://ichthyolith/rois_jpg/abcxyz.jpg
    assert url.startswith('s3://')
    bucket = url.split('/')[2]  # ichthyolith
    key = url.split('/',3)[-1]  # rois_jpg/abcxyz.jpg
    return bucket, key


def s3_client_and_bucket(client_config, bucket=None): # -> (boto3.resources.base.ServiceResource, Union[boto3.resources.base.ServiceResource,None])
    """Instantiates an s3 connection. If already instantiated, nothing happens"""
    if isinstance(client_config, dict):
        config = client_config.copy()
        if bucket is None and 'bucket' in config:
            bucket = config.pop('bucket')
        elif 'bucket' in config:
            config.pop('bucket')
        if 'prefix' in config:
            config.pop('prefix')
        client = boto3.resource('s3', **config)
    else:
        client = client_config  # boto3.resources.factory.s3.Bucket or botocore.client.S3

    if isinstance(bucket,str):
        bucket = client.Bucket(bucket)
    return client, bucket


def s3_list_objects(client_config, bucket=None, prefix=None):
    client, bucket = s3_client_and_bucket(client_config, bucket)

    if prefix is None and 'prefix' in client_config:
        prefix = client_config['prefix']

    object_keys = []
    if prefix:
        for obj in tqdm(bucket.objects.filter(Prefix=prefix)):
            object_keys.append(obj.key)
    else:
        for obj in tqdm(bucket.objects.all()):
            object_keys.append(obj.key)

    return object_keys


def s3_object_exists(client_config, key, bucket=None):
    if key.startswith('s3://'):     # s3://bucket/prefix/abcxyz.jpg
        assert bucket is None
        bucket, key = s3_url_to_bucket_and_key(key)
    else:
        assert bucket is not None

    if isinstance(client_config, dict):
        client, Bucket = s3_client_and_bucket(client_config, bucket)
    else:
        client = client_config
        if isinstance(bucket, str):
            Bucket = client.Bucket(bucket)
        else:
            Bucket = bucket

    obj = client.Object(Bucket.name, key)
    try: obj.load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise e
    return True

