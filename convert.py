import boto3
import ffmpy

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # TODO: replace s3 handler with dynamoDB handler
    # Get the object from the event and show its content type
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

            perform()

            global destination
            destination = 'Converted/'+transportstream
            print "Uploading to s3..."
            s3_client.upload_file('/tmp/'+transportstream, bucket, destination)
        except Exception as e:
            print(e)
            print('ERROR! Key: {} Bucket: {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
            raise e

# Converts video segment
def perform():
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
