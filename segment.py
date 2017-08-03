import boto3
import ffmpy
import os

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
dynamo = boto3.resource('dynamodb')
table = dynamo.Table('FT_SegmentState')
sqs = boto3.resource('sqs')
statusqueue = sqs.get_queue_by_name(QueueName='FT_status_queue')

def lambda_handler(event, context):
    # Get the object from the event and show its content type
    # This job is triggered by FT_VideoConversions

    Bucket = event[0]['dynamodb']['NewImage']['Bucket']
    Key = event[0]['dynamodb']['NewImage']['Key']
    ConversionID = event[0]['dynamodb']['NewImage']['ConversionID']
    QueueMessageID = event[0]['dynamodb']['NewImage']['QueueMessageID']

    print "Bucket/Key is {}{}".format(Bucket, Key)
    print "ConversionID is {}".format(ConversionID)

    if not VideoURL.endswith('/'):
        try:
            # Finagle S3 bucket naming conventions so that boto retrieves the correct file
            global split_key
            split_key = VideoURL.split('/')
            global file_name
            file_name = VideoURL[-1]
            global file_extension
            file_extension = os.path.splitext(file_name)[1]
            print "segmenting {} file".format(file_extension)

            # sqs.put_message(
            # QueueUrl=statusqueue
            # ReceiptHandle=StatusReceipt
            # status='Downloading'
            # )
            # Download the source file from s3
            s3_client.download_file(bucket, VideoURL, '/tmp/'+file_name)


            # sqs.put_message(
            # QueueUrl=statusqueue
            # ReceiptHandle=StatusReceipt
            # status='Ready to process'
            # )

            # Call ffmpy function
            segment()
            global destination

            # Each chunk is uploaded to s3
            print "Uploading segments to s3..."
            for filename in os.listdir('/tmp/SEGMENT*'):
                destination = 'Segmented/{}'.format(filename)
                s3_client.upload_file('/tmp/'+filename, bucket, destination)
            print "Uploading audio to s3"
            audiodestination = 'Audio/{}'.format(file_name)
            s3_client.upload_file('/tmp/'+file_name+'.mp3', bucket, audiodestination)

            # Update status queue
            # sqs.put_message(
            # QueueUrl=statusqueue,
            # MessageID=MessageID
            # status='Processing'
            # )

        except Exception as e:
            print(e)
            print('ERROR! Key: {} Bucket: {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
            raise e


# ffmpy invocation that SEGMENTs the video into chunks
def segment():
    if key is not None:
        f = ffmpy.FFmpeg(
        executable='./ffmpeg/ffmpeg',
        inputs={'/tmp/'+file_name : None},
        outputs={'/tmp/'+file_name+'.mp3': '-c copy'}
        )
        ff = ffmpy.FFmpeg(
        executable='./ffmpeg/ffmpeg',
        inputs={'/tmp/'+file_name : None},
        outputs={'/tmp/SEGMENT%d{}'.format(file_extension): '-acodec copy -c:a libfdk_aac -f segment -vcodec copy -reset_timestamps 1 -map 0'}
        )
        f.run()
        ff.run()
