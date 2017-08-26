import boto3, json, time

def lambda_handler(event, context):

    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='FT_convert_queue')
    dynamo = boto3.resource('dynamodb')
    table = dynamo.Table('FT_VideoConversions')
    #statusqueue = sqs.Queue(sqs.get_queue_by_name(QueueName='FT_status_queue'))

    # Accept message from SQS
    messages = queue.receive_messages(
        AttributeNames=['All'],
        MessageAttributeNames=['string',],
        MaxNumberOfMessages=10,
        VisibilityTimeout=600,
        WaitTimeSeconds=5)

    # Parse messages and write relevant info to FT_VideoConversions
    for m in messages:
        if m is not None:
            # Load SQS Message as dictionary
            body = json.loads(m.body)

            # Assign important variables
            Bucket = body['bucket']
            ConversionID = body['uploadID']
            Path = str(body['path'])
            if not Path:
                Path = 'NULL'
                VideoURL = "https://{}.s3.amazonaws.com/{}{}".format(Bucket, Filename)
            else:
                VideoURL = "https://{}.s3.amazonaws.com/{}{}".format(Bucket, Path, Filename)
            Filename = body['fileName']
            RequestedFormats = body['sizeFormat']
            QueueMessageID = m.message_id
            epochnow = int(time.time())
            print "Bucket={} ConversionID={} Path={} Filename={} RequestedFormats={} VideoURL={} QueueMessageID={} epochnow={}".format(Bucket, ConversionID, Path, Filename, RequestedFormats, VideoURL, QueueMessageID, epochnow)
            # If this job has not been done before, write a new row in DynamoDB, triggering Lambda 2: Segment
            entry = table.get_item(Key={'ConversionID' : ConversionID})
            if 'Item' not in entry:
                response = table.put_item(
                                Item = {
                                    'Bucket': Bucket,
                                    'ConcatReady': 0,
                                    'ConversionID': ConversionID,
                                    'Created': epochnow,
                                    'Filename': Filename,
                                    'Path': Path,
                                    'QueueMessageID': QueueMessageID,
                                    'RequestedFormats': RequestedFormats,
                                    'Retries': 0,
                                    'Updated': epochnow,
                                    'VideoURL': VideoURL
                                }
                            )
                print('PutItem succeeded: {}'.format(json.dumps(response, indent=4)))
                '''
                statusqueue.send_message(
                    MessageBody='Waiting for encoder...',
                    MessageAttributes={
                        'ConversionID': {
                            'StringValue': ConversionID,
                            'DataType': 'String'
                        }
                    }
                )'''
            # Else, increment retries and trigger convert
            elif entry['Item']['Retries'] < 4:
                response = table.update_item(
                                Key={'ConversionID': ConversionID},
                                ExpressionAttributeValues={':val': 1},
                                UpdateExpression='set Retries = Retries + :val')
                print('UpdateItem suceeded:{}'.format(json.dumps(response, indent=4)))
            # If we've failed 3 times or are in some crazy unrecognizable state, move to deadletter queue
            else:
                raise Exception('Redrive policy should have run.')
