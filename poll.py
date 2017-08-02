import boto3, json, time

def lambda_handler(event, context):

    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='FT_convert_queue')
    #statusqueue = sqs.get_queue_by_name(QueueName='FT_status_queue')
    epochnow = int(time.time())

    # Accept message from SQS
    messages = queue.receive_messages(
        AttributeNames=['All'],
        MessageAttributeNames=['string',],
        MaxNumberOfMessages=10,
        VisibilityTimeout=600,
        WaitTimeSeconds=5)

    for m in messages:
        if m is not None:
            # Load SQS Message as dictionary
            body = json.loads(m.body)

            # Assign important variables
            ConversionID = body['uploadID']
            RequestedFormats = body['sizeFormat']
            VideoURL = "https://{}.s3.amazonaws.com/{}{}".format(body['bucket'], body['path'], body['fileName'])
            QueueMessageID = m.message_id

            # Write to DynamoDB
            dynamodb = boto3.resource('dynamodb')
            table = dynamodb.Table('FT_VideoConversions')

            # If this job has not been done before, write a new row in DynamoDB, triggering Lambda 2: Segment
            entry = table.get_item(Key={'ConversionID' : ConversionID})
            if 'Item' not in entry:
                response = table.put_item(
                               Item={
                                    'ConversionID': ConversionID,
                                    'Created': epochnow,
                                    'Updated': epochnow,
                                    'QueueMessageID': QueueMessageID,
                                    'RequestedFormats': RequestedFormats,
                                    'Retries': 0,
                                    'VideoURL': VideoURL
                                })
                print("PutItem succeeded: {}".format(json.dumps(response, indent=4)))
                #sqs.put_message(
                #    QueueUrl=statusqueue
                #    status='Waiting for Encoder')
            # Else, increment retries and trigger convert
            elif entry['Item']['Retries'] < 4:
                response = table.update_item(
                                Key={'ConversionID': ConversionID},
                                ExpressionAttributeValues={':val': 1},
                                UpdateExpression="set Retries = Retries + :val")
                print("UpdateItem suceeded:{}".format(json.dumps(response, indent=4)))
            # If we've failed 3 times or are in some crazy unrecognizable state, move to deadletter queue
            else:
                raise Exception("Redrive policy should have run.")
