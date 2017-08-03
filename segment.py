import boto3, ffmpy, os

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
dynamo = boto3.resource('dynamodb')
table = dynamo.Table('FT_SegmentState')
sqs = boto3.resource('sqs')
statusqueue = sqs.get_queue_by_name(QueueName='FT_status_queue')

def lambda_handler(event, context):
    # Get the object from the event and show its content type
    # This job is triggered by FT_VideoConversions

    Row = event[0]['dynamodb']['NewImage']
    Bucket = Row['Bucket']
    Filename = Row['Filename']
    Path = Row['Path']
    ConversionID = Row['ConversionID']
    QueueMessageID = Row['QueueMessageID']
    S3Path = "{}{}".format(Path, Filename)
    LocalPath = "/tmp/{}/{}".format(ConversionID, Filename)

    print "Bucket/ConversionID is {}, {}".format(Bucket, ConversionID)
    print "QueueMessageID is {}".format(QueueMessageID)

    if not S3Path.endswith('/'):
        try:
            # Finagle S3 bucket naming conventions so that boto retrieves the correct file
            #global split_key
            #split_key = Key.split('/')
            #global file_name
            #file_name = Key[-1]
            #global file_extension
            #file_extension = os.path.splitext(file_name)[1]
            #print "segmenting {} file".format(file_extension)

            # sqs.put_message(
            # QueueUrl=statusqueue
            # ReceiptHandle=StatusReceipt
            # status='Downloading'
            # )
            # Download the source file from s3
            #s3_client.download_file(bucket, Key, '/tmp/'+file_name)
            s3.Bucket(Bucket).download_file(S3Path, LocalPath)

            # sqs.put_message(
            # QueueUrl=statusqueue
            # ReceiptHandle=StatusReceipt
            # status='Ready to process'
            # )

            # Call ffmpy function
            segment(LocalPath)
            global destination

            # Each chunk is uploaded to s3
            FilePath, Extension = os.path.splitext(LocalPath)
            print "Uploading segments to s3..."
            for filename in os.listdir('/tmp/{}/{}*'.format(ConversionID, FilePath)):
                if !filename.endswith('.mp3'):
                    destination = 'Segmented/{}'.format(filename)
                    s3_client.upload_file('/tmp/'+filename, bucket, destination)
            print "Uploading audio to s3"
            audiodestination = 'Audio/{}'.format(file_name)
            s3_client.upload_file('{}.mp3'.format(FilePath), bucket, audiodestination)

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
def segment(path):
    if path is not None:
        FilePath, Extension = os.path.splitext(path)
        f = ffmpy.FFmpeg(
                executable='./ffmpeg/ffmpeg',
                inputs={path : None},
                outputs={FilePath +'.mp3': '-c copy'}
                )
        ff = ffmpy.FFmpeg(
                executable='./ffmpeg/ffmpeg',
                inputs={path : None},
                outputs={'{}SEGMENT%d{}'.format(FilePath, Extension): '-acodec copy -c:a libfdk_aac -f segment -vcodec copy -reset_timestamps 1 -map 0'}
                )
        f.run()
        ff.run()
