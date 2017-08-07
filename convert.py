import boto3
import ffmpy
import json

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
dynamo = boto3.resource('dynamodb')
table = dynamo.Table('FT_SegmentState')
# We don't interact with SQS in this job because it runs in parallel with so many others.

def lambda_handler(event, context):
    # Get the object from the event and show its content type
    # This lambda is triggered from FT_SegmentState

#    print(json.dumps(event, context))
#    for record in event['Records']:
#        print record
    Row = event[0]['dynamodb']['NewImage']
    ConversionID = Row['ConversionID']
    SegmentID = Row['SegmentID']
    Bucket = Row['Bucket']
    Path = Row['Path']
    Filename, Extension = os.path.splitext(Row['Filename'])
    S3Path = "{}{}{}".format(Path, Filename, Extension)
    LocalPath = '/tmp/{}{}'.format(Filename, Extension)

    print "absolutepath is {}{}".format(Path, Filename)
    print "bucket is {}".format(bucket)

    if not key.endswith('/'):
        try:
            # Finagle S3 bucket naming conventions so that boto retrieves the correct file.
            #global split_key
            #split_key = key.split('/')
            #global file_name
            #file_name = split_key[-1]

            print "Downloading source file..."
            #s3_client.download_file(bucket, key, '/tmp/'+file_name)
            s3.Bucket(Bucket).download_file(S3Path, LocalPath)

            transcode(LocalPath)

            destination = '{}/Converted'.format(Path)
            print "Uploading to s3..."
            result = s3_client.upload_file('/tmp/stream/{}.ts'.format(Filename), bucket, destination)
            table.update_item(
            key={
                'SegmentID': SegmentID,
                },
            UpdateExpression="set Completed = 1",
            )

            # Check if all segments are complete: if they are, trigger concat step
            allsegments = table.query(KeyConditionExpression=Key('ConversionID').eq(ConversionID))
            allstatus = allsegments['Completed']
            if checksegments(allstatus):
                nexttable = dynamo.Table('FT_ConversionState')
                nexttable.update_item(
                   Key={
                        'ConversionID': conversionID,
                    },
                    UpdateExpression="set ConcatReady = 1",
                )
        except Exception as e:
            print(e)
            print('ERROR! Key: {} Bucket: {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
            raise e

# Converts video segment
def transcode(path):
    if path is not None:
        FilePath, Extension = os.path.splitext(path)
        Filename = path.split('/')[-1]
        print "Converting File..."
        ff = ffmpy.FFmpeg(
                executable='./ffmpeg/ffmpeg',
                inputs={path : None},
                outputs={'/tmp/converted/{}'.format(Filename) : '-y'})
        ff.run()

        print "Transcoding to lossless transport stream..."
        fff = ffmpy.FFmpeg(
        executable='./ffmpeg/ffmpeg',
        inputs={'/tmp/converted/{}'.format(Filename) : None},
        outputs={'/tmp/stream/{}.ts'.format(Filename) : '-y -c copy -bsf:v h264_mp4toannexb -f mpegts'}
        )
        fff.run()


# Checks if all segments are complete
def checksegments(iterator):
    return all(first == rest for rest in iterator)
