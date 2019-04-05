# Function for Create/Update dynamodb_images_table
def update_compliant_images_db(client, config):

    compliantImages = config.compliantImages
    dynamodb_images_table = config.dynamodb_images_table
    session = client.session

    images = session.resource('ec2').images.filter(Owners=config.trusted_accounts)

    for image in images:
        if config.trusted_ami_prefix in image.name:
            compliantImages.append(image)

    dynamodb_client = session.client('dynamodb')
    for image in compliantImages:
        print(image.id, image.name)
        try:
            response = dynamodb_client.describe_table(
                TableName=dynamodb_images_table
                )

        except dynamodb_client.exceptions.ResourceNotFoundException:
            response = dynamodb_client.create_table(
                TableName=dynamodb_images_table,
                KeySchema=[
                    {
                        'AttributeName': 'ID',
                        'KeyType': 'HASH'
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'ID',
                        'AttributeType': 'S'
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )
            waiter = dynamodb_client.get_waiter('table_exists')
            waiter.wait(TableName=dynamodb_images_table)
            pass

        dynamodb_client.put_item(
            TableName=dynamodb_images_table,
            Item={
                'ID': {
                    'S': str(image.id)
                },
                'Name': {
                    'S': str(image.name)
                },
                'OwnerAlias': {
                    'S': str(image.image_owner_alias)
                },
                'Location': {
                    'S': str(image.image_location)
                },
                'EnaSupport': {
                    'S': str(image.ena_support)
                },
                'CreationDate': {
                    'S': str(image.creation_date)
                },
                'Description': {
                    'S': str(image.description)
                },
                'State': {
                    'S': str(image.state)
                },
                'Tags': {
                    'S': str(image.tags)
                }
            }
        )


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


# Main Function for --update-images-db argument
def images_main(client, config):
    print("Updating image DynamoDB")
    delete_dynamodb(client.session, config.dynamodb_images_table)
    update_compliant_images_db(client, config)
