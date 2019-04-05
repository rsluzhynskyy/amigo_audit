import sys
import os
import re
import gzip
import json
import datetime

images_info = []
compliantImages = []


# Get compliant Images list.
def get_compliant_images(session, dynamodb_images_table):
    dynamodb_client = session.client('dynamodb')
    try:
        response = dynamodb_client.scan(
            TableName=dynamodb_images_table
            )
        for i in response['Items']:
            compliantImages.append(i['ID']['S'])

    except dynamodb_client.exceptions.ResourceNotFoundException:
        print("Unable connect to {0} DynamoDB".format(dynamodb_images_table))


def delete_dynamodb(session, dynamodb_table):
    dynamodb_client = session.client('dynamodb')
    print('deleting table')
    try:
        dynamodb_client.delete_table(TableName=dynamodb_table)
        print("Deleting {0}".format(dynamodb_table))
        waiter = dynamodb_client.get_waiter('table_not_exists')
        waiter.wait(TableName=dynamodb_table)
    except dynamodb_client.exceptions.ResourceNotFoundException:
        print("{0} does not exist".format(dynamodb_table))


# Fuction to get all Account ID/Name
def get_account_id_name(result, paginator, bucket_name):
    count = 0
    aws_account = {}
    aws_accounts = []
    for o in result.search('CommonPrefixes'):
        aws_account = {"account_name": (o.get('Prefix')).split('/')[1]}
        aws_accounts.append(aws_account)
        count = count + 1

    for o in aws_accounts:
        result = paginator.paginate(
            Bucket=bucket_name,
            Prefix="ConfigLogs/"+o['account_name']+"/"+"AWSLogs/",
            Delimiter='/'
            )
        for k in result.search('CommonPrefixes'):
            account_id = {"account_id": (k.get('Prefix')).split('/')[3]}
            o.update(account_id)
    return aws_accounts


# Get latest JSON gzip files for account in all regions.
def get_latest_ConfigSnapshot(account_id, account_name, client, config):
    current_date = config.current_date
    az_regex = "^[a-z]{2}-[a-z]*-[0-9]{1}"
    result = client.paginator.paginate(
        Bucket=config.bucket_name,
        Prefix="ConfigLogs/"+account_name+"/"+"AWSLogs/"+account_id+"/Config/",
        Delimiter='/'
        )
    prefix = "ConfigLogs/{}/AWSLogs/{}/Config/".format(
        account_name,
        account_id
        )
    suffix = "/{}/{}/{}/".format(
        current_date["current_year"],
        current_date["current_month"],
        current_date["current_day"]
        )
    for k in result.search('CommonPrefixes'):
        if re.match(az_regex, (k.get('Prefix')).split('/')[5]):
            aws_region = (k.get('Prefix')).split('/')[5]

            result = client.paginator.paginate(
                Bucket=config.bucket_name,
                Prefix=prefix+aws_region+suffix,
                Delimiter='/'
                )
            get_latest_file(result, account_id, account_name, config, client)


# Get latest file in bucket subdirectory
def get_latest_file(result, account_id, account_name, config, client):
    temp_file = "output/temp.json.gz"
    bucket_name = config.bucket_name
    get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))
    s3_client = client.session.client('s3')

    for prefix in result.search('CommonPrefixes'):
        if prefix is not None:
            if (prefix.get('Prefix')).split('/')[9] == 'ConfigSnapshot':
                last_added = [obj['Key'] for obj in sorted((s3_client).list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=prefix.get('Prefix'))['Contents'],
                    key=get_last_modified
                    )][-1]
                (s3_client).download_file(bucket_name, last_added, temp_file)
                with gzip.open(temp_file, 'rb') as f:
                    pe_json_parser(f, account_id, account_name, config, client)
                if os.path.exists(temp_file):
                    os.remove(temp_file)


# Function for parsing Policy Engine ConfigSnapshot JSON files
def pe_json_parser(json_file, account_id, account_name, config, client):
    json_out = json.load(json_file)
    for i in json_out['configurationItems']:
        ec2InstInfo = {}
        if i.get('ARN'):
            if 'arn:aws:ec2:' in i['ARN'] and 'instance' in i['ARN']:
                name = name_tag_check(i['tags'])
                image_id = i['configuration']['imageId']
                awsRegion = i['awsRegion']
                image_details = get_image_info(image_id, awsRegion, client.session)

                ec2InstInfo = {
                    'ID': i['configuration']['instanceId'],
                    'Name': name,
                    'Type': i['configuration']['instanceType'],
                    'State': i['configuration']['state']['name'],
                    'LaunchTime': i['configuration']['launchTime'],
                    'PrivateIP': i['configuration']['privateIpAddress'],
                    'ImageID': i['configuration']['imageId'],
                    'AvailabilityZone': i['availabilityZone'],
                    'AccountID': account_id,
                    'AccountName': account_name,
                    'Region': i['awsRegion'],
                    'AccountID': i['awsAccountId'],
                    'Platform': image_details['Platform'],
                    'ImageName': image_details['ImageName'],
                    'ImageCreationDate': image_details['ImageCreationDate'],
                    'Tags': i['tags']
                }
            if ec2InstInfo:
                with open("output/instances.out", "a") as file_out:
                    file_out.write(str(ec2InstInfo))
                    file_out.write("\n")
                    file_out.close()
                update_dynamodb(client.session, config, ec2InstInfo)


# Function for Create/Update Instance DynamoDB
def update_dynamodb(session, config, instance_info):
    dynamodb_instances_table = config.dynamodb_instances_table
    dynamodb_client = session.client('dynamodb')

# Create DynamoDB if does not exist
    try:
        response = dynamodb_client.describe_table(TableName=dynamodb_instances_table)
    except dynamodb_client.exceptions.ResourceNotFoundException:
        response = dynamodb_client.create_table(
            TableName=dynamodb_instances_table,
            KeySchema=[
                {
                    'AttributeName': 'ID',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'TagsBU',
                    'KeyType': 'RANGE'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'ID',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'TagsBU',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        waiter = dynamodb_client.get_waiter('table_exists')
        waiter.wait(TableName=dynamodb_instances_table)
        pass

    image_id = instance_info['ImageID']

# ami_status check
    if image_id in compliantImages:
        image_creation_date = datetime.datetime.strptime(str(instance_info["ImageCreationDate"])[:-6], '%Y-%m-%dT%H:%M:%S')
        if config.timeLimit < image_creation_date:
            ami_status = "Current"
        else:
            ami_status = "Deprecated"
    else:
        if instance_info["ImageCreationDate"] is "Unavailable":
            ami_status = "Unavailable"
        else:
            ami_status = "Untracked"


# Check if tags exist or exist but empty
    if "appid" in instance_info['Tags']:
        appid = (instance_info['Tags'])['appid']
        if appid == "":
            appid = "None"
    else:
        appid = "None"

    if "environment" in instance_info['Tags']:
        environment = (instance_info['Tags'])['environment']
        if environment == "":
            environment = "None"
    else:
        environment = "None"

    if "environment" in instance_info['Tags']:
        environment = (instance_info['Tags'])['environment']
    else:
        environment = "None"

    print(
        ami_status,
        image_id,
        instance_info['ID'],
        str((instance_info['Tags']).get('owner', "None")),
        str((instance_info['Tags']).get('bu', "None")),
        str((instance_info['Tags']).get('product', "None")),
        str((instance_info['Tags']).get('component', "None")),
        str((instance_info['Tags']).get('servicename', "None")),
        environment,
        appid,
        )


    dynamodb_client.put_item(
        TableName=dynamodb_instances_table,
        Item={
            'ID': {
                'S': instance_info['ID']
            },
            'Name': {
                'S': instance_info['Name']
            },
            'Type': {
                'S': instance_info['Type']
            },
            'State': {
                'S': instance_info['State']
            },
            'LaunchTime': {
                'S': instance_info['LaunchTime']
            },
            'PrivateIP': {
                'S': instance_info['PrivateIP']
            },
            'ImageID': {
                'S': instance_info['ImageID']
            },
            'AvailabilityZone': {
                'S': instance_info['AvailabilityZone']
            },
            'AccountID': {
                'S': instance_info['AccountID']
            },
            'AccountName': {
                'S': instance_info['AccountName']
            },
            'Region': {
                'S': instance_info['Region']
            },
            'TagsOwner': {
                'S': str((instance_info['Tags']).get('owner', "None"))
            },
            'TagsBU': {
                'S': str((instance_info['Tags']).get('bu', "None"))
            },
            'TagsProduct': {
                'S': str((instance_info['Tags']).get('product', "None"))
            },
            'TagsComponent': {
                'S': str((instance_info['Tags']).get('component', "None"))
            },
            'TagsServiceName': {
                'S': str((instance_info['Tags']).get('servicename', "None"))
            },
            'TagsAppid': {
                'S': str(appid)
            },
            'Platform': {
                'S': str(instance_info['Platform'])
            },
            'ImageName': {
                'S': str(instance_info['ImageName'])
            },
            'ImageCreationDate': {
                'S': str(instance_info['ImageCreationDate'])
            },
            'AmiStatus': {
                'S': str(ami_status)
            }
        }
    )


# Fuction for getting image details if available.
def get_image_info(image_id, region, session):
    ec2ImageInfo = {}
    if next((item for item in images_info if item["ImageID"] == image_id), False):
        ec2ImageInfo = next((item for item in images_info if item["ImageID"] == image_id))
    else:
        ec2 = session.client('ec2', region_name=region)
        image_info = ec2.describe_images(
            Filters=[{'Name': "image-id", 'Values': [image_id]}]
            )
        ImageName = "None"
        ImageCreationDate = "Unavailable"
        platform = "None"
        for info in image_info['Images']:
            if info['CreationDate']:
                ImageName = info['Name']
                ImageCreationDate = info['CreationDate']
                try:
                    platform = info['Platform']
                except KeyError:
                    platform = "linux"
        ec2ImageInfo = {
            "ImageID": image_id,
            "Platform": platform,
            "ImageName": ImageName,
            "ImageCreationDate": ImageCreationDate
        }
        images_info.append(ec2ImageInfo)

    return(ec2ImageInfo)

def CleanupOutput(output_dir):
    try:
        os.stat(output_dir)
    except:
        os.mkdir(output_dir)

    for f in os.listdir(output_dir):
        if os.path.exists(output_dir+"/"+f):
            print(f)
            os.remove(output_dir+"/"+f)

# Check for Name Tag
def name_tag_check(tags):
    if tags is None:
        name = "NoName"
    else:
        try:
            if tags['Name']:
                name = tags['Name']
        except KeyError as e:
            name = "NoName"
    return(name)


# Main Function for --update-instances-db argument
def instances_main(client, config):
    get_compliant_images(client.session, config.dynamodb_images_table)
    CleanupOutput("output")
    delete_dynamodb(client.session, config.dynamodb_instances_table)
    aws_accounts = get_account_id_name(client.result, client.paginator, config.bucket_name)
    for o in aws_accounts:
        get_latest_ConfigSnapshot(o["account_id"], o['account_name'], client, config)