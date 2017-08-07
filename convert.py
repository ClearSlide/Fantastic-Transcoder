import boto3, ffmpy, json

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
dynamo = boto3.resource('dynamodb')
table = dynamo.Table('FT_SegmentState')

# Triggered by write to FT_SegmentState
def lambda_handler(event, context):

    # Load triggering row from FT_SegmentState and assign variables
    Row = event[0]['dynamodb']['NewImage']
    Bucket = Row['Bucket']
    ConversionID = Row['ConversionID']
    Path = Row['Path']
    SegmentID = Row['SegmentID']
    S3Path = Row['Filename']
    Filename, Extension = os.path.splitext(S3Path)
    LocalPath = '/tmp/{}{}'.format(Filename, Extension)

    print 'Converting {} with ConversionID: {}, in Bucket: {}'.format(Filename, ConversionID, Bucket)

    try:
        print 'Downloading segment and transcoding'
        s3.Bucket(Bucket).download_file(S3Path, LocalPath)
        transcode(LocalPath)

        # File looks like "videofileSEGMENT123.ts"
        print 'Uploading to s3...'
        s3_client.upload_file('/tmp/stream/{}.ts'.format(Filename), bucket, '{}.ts'.format(Filename))
        table.update_item(
        key={
            'SegmentID': SegmentID,
            },
        UpdateExpression='set Completed = 1',
        )

        # Check if all segments are complete: if they are, trigger concat
        allsegments = table.query(KeyConditionExpression=Key('ConversionID').eq(ConversionID))
        allstatus = allsegments['Completed']
        if all(first == rest for rest in allstatus):
            nexttable = dynamo.Table('FT_ConversionState')
            nexttable.update_item(
               Key={
                    'ConversionID': conversionID,
                },
                UpdateExpression='set ConcatReady = 1',
            )
    except Exception as e:
        raise Exception('Failure during conversion for segment {} in {}!').format(Filename, Bucket)

# Converts video segment
def transcode(path):
    if path is not None:
        FilePath, Extension = os.path.splitext(path)
        Filename = path.split('/')[-1]
        print 'Converting File...'
        ff = ffmpy.FFmpeg(
                executable='./ffmpeg/ffmpeg',
                inputs={path : None},
                outputs={'/tmp/converted/{}'.format(Filename) : '-y'})
        ff.run()

        print 'Transcoding to lossless transport stream...'
        fff = ffmpy.FFmpeg(
        executable='./ffmpeg/ffmpeg',
        inputs={'/tmp/converted/{}'.format(Filename) : None},
        outputs={'/tmp/stream/{}.ts'.format(Filename) : '-y -c copy -bsf:v h264_mp4toannexb -f mpegts'}
        )
        fff.run()
