import boto3
import json
import argparse
import datetime
from handlers import instances_compliance
from handlers import images_compliance


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--profile', default=None, help="AWS Profile")
    parser.add_argument('-r', '--region', default=None, help="AWS Profile")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '-i', '--update-images-db',
        action="store_true",
        default=False,
        help="Update djif-compliantImages DynamodDB with compliant images"
        )
    group.add_argument(
        '-e', '--update-instances-db',
        action="store_true",
        default=False,
        help="Update djif-ec2instances DynamoDB with latest instance status"
        )

    args = parser.parse_args()
    return args


def cli(profile, region):
    """Manages snapshots"""
    if profile is None:
        profile = 'default'
    try:
        session = boto3.Session(profile_name=profile, region_name=region)
    except botocore.exceptions.ProfileNotFound as e:
        if region is None:
            region = 'us-east-1'
        session = boto3.Session(region_name=region)
    return(session)


class Config:
    def __init__(self, config_file):
        with open(config_file) as json_data_file:
            config_data = json.load(json_data_file)

        self.images_info = config_data["images_info"]
        self.compliantImages = config_data["compliantImages"]
        self.bucket_name = config_data["bucket_name"]
        self.dynamodb_instances_table = config_data["dynamodb_instances_table"]
        self.dynamodb_images_table = config_data["dynamodb_images_table"]
        self.trusted_accounts = config_data["trusted_accounts"]
        self.trusted_ami_prefix = config_data["trusted_ami_prefix"]
        deprecated_threshold = config_data["deprecated_threshold"]
        self.timeLimit = datetime.datetime.now().replace(microsecond=0) - datetime.timedelta(days=deprecated_threshold)
        self.current_date = {
            "current_year": str(datetime.date.today().year),
            "current_month": str(datetime.date.today().month),
            "current_day": str(datetime.date.today().day)
        }


class Client:
    def __init__(self, args, bucket_name):
        self.session = cli(args.profile, args.region)
        self.paginator = (self.session.client('s3')).get_paginator('list_objects')
        self.result = self.paginator.paginate(
            Bucket=bucket_name,
            Prefix='ConfigLogs/',
            Delimiter='/'
            )


if __name__ == '__main__':
    args = get_args()
    config = Config("./config/config.json")
    client = Client(args, config.bucket_name)
    if args.update_instances_db is True:
        instances_compliance.instances_main(client, config)
    if args.update_images_db is True:
        images_compliance.images_main(client, config)
