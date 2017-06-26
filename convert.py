import boto3
import ffmpy
import json

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
dynamo = boto3.resource('dynamodb')
table = dynamo.Table('FT_SegmentState')

def lambda_handler(event, context):
    # Get the object from the event and show its content type

#    print(json.dumps(event, context))
#    for record in event['Records']:
#        print record
    ConversionID = event['ConversionID']
    SegmentID = event['SegmentID']
#    response = table.get_item(
#    Key={
#        'conversionID': conversion,
#        'segmentID': segment
#    }

    global bucket
    bucket = event['Records'][0]['s3']['bucket']['name']
    global key
    key = event['Records'][0]['s3']['object']['key']

    print "key is {}".format(key)
    print "bucket is {}".format(bucket)

    if not key.endswith('/'):
        try:
            # Finagle S3 bucket naming conventions so that boto retrieves the correct file.
            global split_key
            split_key = key.split('/')
            global file_name
            file_name = split_key[-1]
            print "Downloading source file..."
            s3_client.download_file(bucket, key, '/tmp/'+file_name)

            transcode()

            global destination
            destination = 'Converted/'+transportstream
            print "Uploading to s3..."
            s3_client.upload_file('/tmp/'+transportstream, bucket, destination)

            segmentstatus = table.get_item(hash_key=SegmentID)
            # Update SegmentState with segment completion
            # Check if all segments are complete: if they are, trigger concat step
            allsegments = table.query(KeyConditionExpression=Key('ConversionID').eq(ConversionID))
            allstatus = allsegments['Completed']
            if checksegments(allstatus):
                nexttable = dynamo.Table('FT_VideoConversions')
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
def transcode():
    if key is not None:
        print "Converting File..."
        global convertedfile
        convertedfile = file_name+'CONVERTED.mp4'
        ff = ffmpy.FFmpeg(
        executable='./ffmpeg/ffmpeg',
        inputs={'/tmp/'+file_name : None},
        outputs={'/tmp/'+convertedfile : '-y'}
        )
        ff.run()
        print "Transcoding to lossless transport stream..."
        global transportstream
        transportstream = file_name+'.ts'
        fff = ffmpy.FFmpeg(
        executable='./ffmpeg/ffmpeg',
        inputs={'/tmp/'+convertedfile : None},
        outputs={'/tmp/'+transportstream : '-y -c copy -bsf:v h264_mp4toannexb -f mpegts'}
        )
        fff.run()


# Checks if all segments are complete
def checksegments(iterator):
    iterator = iter(iterator)
    try:
        first = next(iterator)
    except StopIteration:
        return True
    return all(first == rest for rest in iterator)
