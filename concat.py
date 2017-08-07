import boto3
import ffmpy
import os

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
dynamo = boto3.resource('dynamodb')
table = dynamo.Table('FT_VideoConversions')
sqs = boto3.client('sqs')
queue = sqs.get_queue_by_name(QueueName='FT_convert_queue')
statusqueue = sqs.get_queue_by_name(QueueName='FT_status_queue')

def lambda_handler(event, context):
    # This job is triggered by FT_ConversionState
    # TODO: replace s3 handler with dynamoDB handler
    # Get the objects from the event and show its content type
    # global bucket
    # bucket = event['Records'][0]['s3']['bucket']['name']
    # global key
    # key = event['Records'][0]['s3']['object']['key']

    #global conversionID
    # Get conversionID from dynamoDB
    #conversionID = "Temp"
    #conversionbucket = s3.get_bucket(bucket)

    Row = event[0]['dynamodb']['NewImage']
    ConversionID = Row['ConversionID']
    Bucket = Row['Bucket']
    Path = Row['Path']
    Filename, Extension = os.path.splitext(Row['Filename'])
    S3Path = "{}{}{}".format(Path, Filename, Extension)
    LocalPath = "/tmp/{}/{}{}".format(ConversionID, Filename, Extension)

    print "path is {}".format(Path)
    print "bucket is {}".format(Bucket)
    print "conversionID is {}".format(ConversionID)


    Bucket = s3.Bucket(Bucket)
    
    if not S3Path.endswith('/'):
        try:
            # Finagle S3 bucket naming conventions so that boto retrieves the correct file.
            print "Downloading source files..."

            segments = table.query(KeyConditionExpression=Key('ConversionID').eq(ConversionID))

            for segment_file in segments:
                #global split_key
                split_segment = segment_file.split('/')
                #global file_name
                segment_name = split_segment[-1]

                s3_client.download_file(Bucket, segment_file, '/tmp/' + segment_name)
            print "Downloading audio file..."
            # Is this good enough? Or should we log/track the audio file in dynamo?
            s3_client.download_file(Bucket, 'audio' + ConversionID + '.mp3', '/tmp/' + ConversionID + '.mp3')

            # Verify that the current number of segments have been downloaded
            sqs.put_message(
            QueueUrl=statusqueue
            ReceiptHandle=StatusReceipt
            status='Saving'
            )

            # concat
            output_name = concat(Path, ConversionID)

            # upload to destination
            destination = 'Concatenated/' + Filename
            print "Uploading completed file to s3..."
            s3_client.upload_file('/tmp/' + output_name, Bucket, destination)
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
            print('ERROR! Path: {} Bucket: {}. Make sure they exist and your bucket is in the same region as this function.'.format(Path, Bucket))
            raise e

# Converts video segment
def concat(Path, ConversionID):
    if Path is not None:
        file = open('/tmp/targetlist.txt', w)
        for each in sorted(os.listdir('/tmp/*.ts')):
            file.write(each)
                #writes ordered list of transport streams to file
        print "Concatenating video..."
        ff = ffmpy.FFmpeg(
        executable='./ffmpeg/ffmpeg',
        inputs={'/tmp/targetlist.txt' : '-f concat -safe 0'},
        outputs={'/tmp/' + Path : '-y -c copy -bsf:a aac_adtstoasc'}
        )
        ff.run()

        output_name = '/tmp/' + Path + '-merged'
        fff=ffmpy.FFmpeg(
        executable='./ffmpeg/ffmpeg',
        inputs={
        '/tmp/' + Path : [None],
        '/tmp/' + ConversionID + '.mp3' : [None]
        },
        outputs={'/tmp/' + output_name : '-c:v copy -c:a aac -strict experimental'}
        )

        file.close()
        return output_name
