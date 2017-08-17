import boto3, ffmpy, json, os
from boto3.dynamodb.conditions import Key, Attr

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
dynamo = boto3.resource('dynamodb')
table = dynamo.Table('FT_SegmentState')

# Triggered by write to FT_SegmentState
def lambda_handler(event, context):

    # Load triggering row from FT_SegmentState and assign variables
    try:
        Row = event['Records'][0]['dynamodb']['NewImage']
        Bucket = Row['Bucket']['S']
        ConversionID = Row['ConversionID']['S']
        Filename, Extension = os.path.splitext(Row['Filename']['S'])
        Path = Row['Path']['S']
        SegmentID = Row['SegmentID']['S']
    except KeyError:
        print "DynamoDB records are incomplete!"
    else:
        try:
            os.makedirs('/tmp/converted')
            os.makedirs('/tmp/stream')
        except Exception as FilesystemError:
            print "Directories already exist? Lambda is reusing a container. {}".format(FilesystemError)
        if Path == 'NULL':
            S3Path = '{}{}'.format(Filename, Extension)
        elif Path != 'NULL':
            S3Path = '{}{}{}'.format(Path, Filename, Extension)
        LocalPath = '/tmp/{}{}'.format(Filename, Extension)

        print 'Converting {} with ConversionID: {}, in Bucket: {}'.format(Filename, ConversionID, Bucket)
        print 'Downloading segment and transcoding'
        try:
            s3.Bucket(Bucket).download_file(S3Path, LocalPath)
        except Exception as S3DownloadError:
            print "S3 Download failed. Check region, permissions, etc... {}".format(S3DownloadError)

        transcode(LocalPath)

        # File looks like "videofileSEGMENT123.ts"
        print 'Uploading to s3...'
        try:
            s3_client.upload_file('/tmp/stream/{}.ts'.format(Filename), bucket, '{}.ts'.format(Filename))
        except Exception as S3UploadError:
            print "S3 Upload of {} failed. Check region, permissions, etc... {}".format(Filename, S3DownloadError)

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
        segments = table.query(KeyConditionExpression=Key('ConversionID').eq(ConversionID))['Items']
        if all(s['Completed'] for s in segments):
            nexttable = dynamo.Table('FT_ConversionState')
            try:
                nexttable.update_item(
                   Key={
                        'ConversionID': conversionID,
                    },
                    UpdateExpression='set ConcatReady = 1',
                )
            except Exception as DynamoUpdateError:
                print "DynamoDB update of FT_ConversionState failed. Check table exists, permissions, etc... {}".format(DynamoUpdateError)
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
                outputs={'/tmp/stream/{}.ts'.format(Filename) : '-y -c copy -bsf:v h264_mp4toannexb -f mpegts'})
        fff.run()
