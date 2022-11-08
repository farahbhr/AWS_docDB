import logging
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
# Create the DynamoDB table.
def create_table_useres(dynamodb):
    try:
        table = dynamodb.create_table(
            TableName='users',
            KeySchema=[
                {
                    'AttributeName': 'username',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'last_name',
                    'KeyType': 'RANGE'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'username',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'last_name',
                    'AttributeType': 'S'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )

        # Wait until the table exists.
        table.meta.client.get_waiter('table_exists').wait(TableName='users')

        # Print out some data about the table.
        print(table.item_count)

    except dynamodb.exceptions.ResourceInUseException:
         print("table already exists")
         logging.error("Exception occurred", exc_info=True)
    pass

#Creating a new item
def insert_item(table):
    try:
        table.put_item(
        Item={
                'username': 'janedoe',
                'first_name': 'Jane',
                'last_name': 'Doe',
                'age': 25,
                'account_type': 'standard_user',
            }
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            print("User already exists")
        else:
            print("Unexpected error: %s" % e)

#Getting an item
def get(table):
    try :
        response = table.get_item(
            Key={
                'username': 'janedoe',
                'last_name': 'Doe'
            }
        )
    except ClientError as e:
        print("exception")
        print(e.response['Error']['Message'])
    else:
        item = response['Item']
        print(item)


#Updating an item
def update(table):
    table.update_item(
        Key={
            'username': 'janedoe',
            'last_name': 'Doe'
        },
        UpdateExpression='SET age = :val1',
        ExpressionAttributeValues={
            ':val1': 26
        }
    )
    response = table.get_item(
        Key={
            'username': 'janedoe',
            'last_name': 'Doe'
        }
    )
    item = response['Item']
    print(item)

#deleting an item
def delete_user(table):
    try:
        response = table.delete_item(
            Key={
                'username': 'janedoe',
                'last_name': 'Doe'
            },
            # Conditional request
            ConditionExpression="age <= :value",
            ExpressionAttributeValues={
                ":value": 26
            }
        )
    except ClientError as er:
        if er.response['Error']['Code'] == "ConditionalCheckFailedException":
            print(er.response['Error']['Message'])
            logging.error("Exception occurred", exc_info=True)
        else:
            raise
    else:
        return response


def delete(table):
    table.delete_item(
        Key={
            'username': 'janedoe',
            'last_name': 'Doe'
        }
    )
#Batch writing
def batch(table):
    with table.batch_writer() as batch:
        batch.put_item(
            Item={
                'account_type': 'standard_user',
                'username': 'johndoe',
                'first_name': 'John',
                'last_name': 'Doe',
                'age': 25,
                'address': {
                    'road': '1 Jefferson Street',
                    'city': 'Los Angeles',
                    'state': 'CA',
                    'zipcode': 90001
                }
            }
        )
        batch.put_item(
            Item={
                'account_type': 'super_user',
                'username': 'janedoering',
                'first_name': 'Jane',
                'last_name': 'Doering',
                'age': 40,
                'address': {
                    'road': '2 Washington Avenue',
                    'city': 'Seattle',
                    'state': 'WA',
                    'zipcode': 98109
                }
            }
        )
        batch.put_item(
            Item={
                'account_type': 'standard_user',
                'username': 'bobsmith',
                'first_name': 'Bob',
                'last_name':  'Smith',
                'age': 18,
                'address': {
                    'road': '3 Madison Lane',
                    'city': 'Louisville',
                    'state': 'KY',
                    'zipcode': 40213
                }
            }
        )
        batch.put_item(
            Item={
                'account_type': 'super_user',
                'username': 'alicedoe',
                'first_name': 'Alice',
                'last_name': 'Doe',
                'age': 27,
                'address': {
                    'road': '1 Jefferson Street',
                    'city': 'Los Angeles',
                    'state': 'CA',
                    'zipcode': 90001
                }
            }
        )
#Querying and scanning
#1. all of the users whose username key equals johndoe
def query_username(table):
    from boto3.dynamodb.conditions import Key, Attr
    response = table.query(
        KeyConditionExpression=Key('username').eq('johndoe')
    )
    items = response['Items']
    print(items)
#2.	all the users whose age is less than 27
def query_age(table):
    response = table.scan(
        FilterExpression=Attr('age').lt(27)
    )
    items = response['Items']
    print(items)
#3.	all users whose first_name starts with J and whose account_type is super_user
def query_chain(table):
    response = table.scan(
        FilterExpression=Attr('first_name').begins_with('J') & Attr('account_type').eq('super_user')
    )
    items = response['Items']
    print(items)

#4.	 all users whose state in their address is CA
def query_address(table):
    response = table.scan(
        FilterExpression=Attr('address.state').eq('CA')
    )
    items = response['Items']
    print(items)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    dynamo = boto3.client('dynamodb')
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('users')

    logging.basicConfig(
        filename='app.log',
        level=logging.ERROR,
        format=f'%(asctime)s %(levelname)s %(message)s'
    )

    logger = logging.getLogger()
    logger.debug('The script is starting.')
    logger.info('Connecting to EC2...')
    create_table_useres(dynamo)
    insert_item(table)
    #get(table)
    #update(table)
    delete_user(table)
    #batch(table)
    #query_username(table)
    #query_age(table)
    #query_chain(table)
    #table.delete()