import boto3, ffmpy, os

s3 = boto3.resource('s3')
dynamo = boto3.resource('dynamodb')
table = dynamo.Table('FT_SegmentState')
sqs = boto3.resource('sqs')
#statusqueue = sqs.get_queue_by_name(QueueName='FT_status_queue')

# Triggered by write to FT_VideoConversions
def lambda_handler(event, context):

    # Load triggering row from FT_VideoConversions and assign variables
    Row = event[0]['dynamodb']['NewImage']
    Bucket = Row['Bucket']
    ConversionID = Row['ConversionID']
    Filename, Extension = os.path.splitext(Row['Filename'])
    Path = Row['Path']
    StatusQueueMessageID = Row['QueueMessageID']

    os.makedirs('/tmp/{}'.format(ConversionID))
    S3Path = '{}{}{}'.format(Path, Filename, Extension)
    LocalPath = '/tmp/{}/{}{}'.format(ConversionID, Filename, Extension)

    print 'Bucket/ConversionID is {}, {}'.format(Bucket, ConversionID)
    print 'StatusQueueMessageID is {}'.format(StatusQueueMessageID)

    try:
        s3.Bucket(Bucket).download_file(S3Path, LocalPath)

        # Segment video with ffmpeg
        segment(LocalPath)

        # Upload each segment to S3
        FilePath, Extension = os.path.splitext(LocalPath)
        print 'Uploading segments and audio to s3...'
        for filename in os.listdir('/tmp/{}/'.format(ConversionID)):
            s3.Bucket(Bucket).upload_file('/tmp/{}/{}'.format(ConversionID, filename), '{}{}'.format(Path, filename))
            if filename.endswith('mp3'):
                SegmentID = '-1'
            else:
                segments = os.path.splitext(filename)[0].split('SEGMENT')
                SegmentID = segments[len(segments) - 1]

            # Write to FT_SegmentState
            response = table.put_item(
                            Item = {
                                'Bucket': Bucket,
                                'ConversionID': ConversionID,
                                'Completed': 0,
                                'Filename': filename,
                                'Path': Path,
                                'QueueMessageID': QueueMessageID,
                                'RequestedFormats': RequestedFormats,
                                'SegmentID': SegmentID,
                            }
                        )
            print('PutItem succeeded: {}'.format(json.dumps(response, indent=4)))

    except Exception as e:
        raise Exception('Failure during segmentation for bucket {}!'.format(Bucket))

# ffmpy invocation that SEGMENTs the video into chunks
def segment(path):
    if path is not None:
        FilePath, Extension = os.path.splitext(path)
        f = ffmpy.FFmpeg(
                executable='./ffmpeg/ffmpeg',
                inputs={path : None},
                outputs={FilePath +'.mp3': '-c copy'})
        ff = ffmpy.FFmpeg(
                executable='./ffmpeg/ffmpeg',
                inputs={path : None},
                outputs={'{}SEGMENT%d{}'.format(FilePath, Extension): '-acodec copy -c:a libfdk_aac -f segment -vcodec copy -reset_timestamps 1 -map 0'})
        f.run()
        ff.run()
        os.remove(path)
