import boto3
import json
import time

def lambda_handler(event, context):

    sqs = boto3.client('sqs')
    queue = sqs.get_queue_by_name(QueueName='FT_convert_queue')
    statusqueue = sqs.get_queue_by_name(QueueName='FT_status_queue')
    epochnow = int(time.time())
    # Accept message from SQS
    message = sqs.receive_messages(
        QueueUrl=queue,
        MessageID=messageID
        ReceiptHandle=ReceiptHandle
        AttributeNames=[
            'All'
        ],
        MessageAttributeNames=[
            'string',
        ],
        MaxNumberOfMessages=100,
        VisibilityTimeout=600,
        WaitTimeSeconds=5,
        )
    print message
    # TODO: get conversionID from SQS message

    # Write to DynamoDB
    db = boto3.resource('dynamodb')
    table = dynamodb.Table('FT_VideoConversions')

    # TODO: This should be each because we can get multiple messages.
    # Check if this job has been done before.
    exists = table.get_item(hash_key=conversionID)

    # If we have not been here before, create a new row in DynamoDB. This triggers Lambda 2: Segment
    if exists is None:
        table.put_item(
           Item={
                'ConversionID': conversionID,
                'created': epochnow,
                'retries': 0
            }
        )
        print("PutItem succeeded:")
        print(json.dumps(response, indent=4, cls=DecimalEncoder))
        sqs.put_message(
        QueueUrl=statusqueue
        ReceiptHandle=StatusReceipt
        status='Waiting for Encoder'

        )
    # If we have been here before, increment retries. This still triggers convert
    else if retries < 4:
        table.update_item(
            Key={
                'ConversionID': conversionID
            },
            UpdateExpression="set retries = retries + :val",
            ExpressionAttributeValues={
                ':val': decimal.Decimal(1),
            },
            'updated': epochnow
        )
    else:
    # If we've failed 3 times or are in some crazy unrecognizable state, move to deadletter queue

        sqs.delete_message(
        QueueUrl=queue,
        ReceiptHandle=ReceiptHandle
        )

        sqs.put_message(
        QueueURL=deadletterqueue
        # Figure out how this works
        )

        sqs.put_message(
        QueueUrl=statusqueue
        ReceiptHandle=StatusReceipt
        status='failed'

        )
