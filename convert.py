import boto3, ffmpy, json, os
from boto3.dynamodb.conditions import Key, Attr

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
dynamo = boto3.resource('dynamodb')
table = dynamo.Table('FT_SegmentState')
# Uncomment for debug logging
#boto3.set_stream_logger(name='botocore')

# Triggered by write to FT_SegmentState
def lambda_handler(event, context):
    # Load triggering row from FT_SegmentState and assign variables
    try:
        print "Loading records from DynamoDB."
        Row = event['Records'][0]['dynamodb']['NewImage']
        Bucket = Row['Bucket']['S']
        ConversionID = Row['ConversionID']['S']
        Filename, Extension = os.path.splitext(Row['Filename']['S'])
        Path = Row['Path']['S']
        SegmentID = Row['SegmentID']['S']
        RequestedFormats = Row['RequestedFormats']['M']
    except KeyError:
        print "DynamoDB records are incomplete!"
    else:
        # Figure out if someone's transcoding in the root part of the bucket because dynamoDB hates null strings
        if Path == 'NULL':
            S3Path = '{}{}'.format(Filename, Extension)
        elif Path != 'NULL':
            S3Path = '{}{}{}'.format(Path, Filename, Extension)
        LocalPath = '/tmp/{}{}'.format(Filename, Extension)

        print 'Begin Converting {} with ConversionID: {}, in Bucket:{}'.format(Filename, ConversionID, Bucket)
        print 'Downloading segment and transcoding'
        try:
            s3.Bucket(Bucket).download_file(S3Path, LocalPath)
        except Exception as S3DownloadError:
            print "S3 Download failed. Check region, permissions, etc... {}".format(S3DownloadError)

        print 'entering transcode phase'
        transcode(LocalPath)

        # File looks like "videofileSEGMENT123.ts"
        print 'Uploading converted file to s3...'
        try:
            s3_client.upload_file('/tmp/{}.ts'.format(Filename), Bucket, '{}.ts'.format(Filename))
        except Exception as S3UploadError:
            print "S3 Upload of {} failed. Check region, permissions, etc... {}".format(Filename, S3UploadError)

        try:
            table.update_item(
                Key={
                    'SegmentID': SegmentID,
                    },
                UpdateExpression='set Completed = 1',
            )
        except Exception as DynamoUpdateError:
            print "DynamoDB update of FT_SegmentState failed. Check table exists, permissions, etc... {}".format(DynamoUpdateError)

        # Check if all segments are complete: if they are, trigger concat
        segments = table.query(IndexName='SegmentID-ConversionID-index',KeyConditionExpression=Key('ConversionID').eq(ConversionID))['Items']
        if all(s['Completed'] for s in segments):
            nexttable = dynamo.Table('FT_ConversionState')
            try:
                print "All segments converted! Updating DynamoDB with concatReady!"
                nexttable.update_item(
                   Key={
                        'ConversionID': conversionID,
                    },
                    UpdateExpression="SET ConcatReady = :updated",
                    ExpressionAttributeValues={':updated': '1'}
                )
            except Exception as DynamoUpdateError:
                print "DynamoDB update of FT_ConversionState failed. Check table exists, permissions, etc... {}".format(DynamoUpdateError)
# Converts video segment
def transcode(path):
    if path is not None:
        FilePath, Extension = os.path.splitext(path)
        Filename = path.split('/')[-1]
        convertedfile = Filepath+'CONVERTED.mp4'
        print 'Converting File {} to Requested format with destination {}'.format(path, convertedfile)
        try:
            ff = ffmpy.FFmpeg(
                executable='./ffmpeg/ffmpeg',
                inputs={path : None},
                outputs={'/tmp/'+convertedfile : '-loglevel 100 -y -c:v libx264 -an'})
            ff.run()
        except Exception as FFFail:
            print "something went wrong during transcoding step! {}".format(FFFail)

        print 'Transcoding to lossless transport stream...'
        try:
            fff = ffmpy.FFmpeg(
                    executable='./ffmpeg/ffmpeg',
                    inputs={'/tmp/'+convertedfile : None},
                    outputs={'/tmp/'+convertedfile+'.ts' : '-loglevel 100 -y -c copy -bsf:v h264_mp4toannexb -f mpegts'})
            fff.run()
        except Exception as FFFail:
            print "something went wrong during transport step! {}".format(FFFail)
