import boto3
import ffmpy

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
dynamo = boto3.resource('dynamodb')
table = dynamo.Table('FT_VideoConversions')
sqs = boto3.client('sqs')
queue = sqs.get_queue_by_name(QueueName='FT_convert_queue')
statusqueue = sqs.get_queue_by_name(QueueName='FT_status_queue')

def lambda_handler(event, context):
    # TODO: replace s3 handler with dynamoDB handler
    # Get the objects from the event and show its content type
    # global bucket
    # bucket = event['Records'][0]['s3']['bucket']['name']
    # global key
    # key = event['Records'][0]['s3']['object']['key']

    global conversionID
    # Get conversionID from dynamoDB
    conversionID = "Temp"
    conversionbucket = s3.get_bucket(bucket)

    print "key is {}".format(key)
    print "bucket is {}".format(bucket)
    print "conversionID is {}".format(conversionID)

    if not key.endswith('/'):
        try:
            # Finagle S3 bucket naming conventions so that boto retrieves the correct file.
            print "Downloading source files..."
            for targetfile in list(conversionbucket.list("Converted/"+conversionID, "")):
                global split_key
                split_key = targetfile.split('/')
                global file_name
                file_name = split_key[-1]

                s3_client.download_file(bucket, targetfile, '/tmp/'+file_name)
            print "Downloading audio file..."
            s3_client.download_file(bucket, 'audio'+conversionID+'.mp3', '/tmp/'+conversionID+'.mp3')
            # Verify that the current number of segments have been downloaded

            sqs.put_message(
            QueueUrl=statusqueue
            ReceiptHandle=StatusReceipt
            status='Saving'
            )
            concat()

            global destination
            destination = 'Concatenated/'+file_name
            print "Uploading to s3..."
            s3_client.upload_file('/tmp/'+convertedfile, bucket, destination)
            sqs.put_message(
            QueueUrl=statusqueue
            ReceiptHandle=StatusReceipt
            status='Finished'
            )
            # Delete message from SQS queue upon successful upload to s3
            sqs.delete_message(
            QueueUrl=queue,
            ReceiptHandle=ReceiptHandle
            )
        except  Exception as e:
            print(e)
            print('ERROR! Key: {} Bucket: {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
            raise e

# Converts video segment
def concat():
    if key is not None:
        file = open('/tmp/targetlist.txt', w)
        for each in sorted(os.listdir('/tmp/*.ts')):
            file.write(each)
                #writes ordered list of transport streams to file
        print "Concatenating video..."
        ff = ffmpy.FFmpeg(
        executable='./ffmpeg/ffmpeg',
        inputs={'/tmp/targetlist.txt' : '-f concat -safe 0'},
        outputs={'/tmp/'+key : '-y -c copy -bsf:a aac_adtstoasc'}
        )
        ff.run()
        fff=ffmpy.FFmpeg(
        executable='./ffmpeg/ffmpeg',
        inputs={
        '/tmp/'+key : [None],
        '/tmp/'+conversionID+'.mp3' : [None]
        },
        outputs={'/tmp/'+key+'-merged' : '-c:v copy -c:a aac -strict experimental'}
        )
