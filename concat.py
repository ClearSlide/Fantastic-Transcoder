import boto3
import ffmpy

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # TODO: replace s3 handler with dynamoDB handler
    # Get the objects from the event and show its content type
    global bucket
    bucket = event['Records'][0]['s3']['bucket']['name']
    global key
    key = event['Records'][0]['s3']['object']['key']
    global uploadID
    # Get uploadID from dynamoDB
    uploadID = "Temp"
    conversionbucket = s3.get_bucket(bucket)

    print "key is {}".format(key)
    print "bucket is {}".format(bucket)
    print "uploadID is {}".format(uploadID)

    if not key.endswith('/'):
        try:
            # Finagle S3 bucket naming conventions so that boto retrieves the correct file.
            for targetfile in list(conversionbucket.list("Converted/"+uploadID, "")):
                global split_key
                split_key = targetfile.split('/')
                global file_name
                file_name = split_key[-1]
                print "Downloading source files..."
                s3_client.download_file(bucket, targetfile, '/tmp/'+file_name)
            # Verify that the current number of segments have been downloaded
            concat()

            global destination
            destination = 'Concatenated/'+file_name
            print "Uploading to s3..."
            s3_client.upload_file('/tmp/'+convertedfile, bucket, destination)

            # Delete message from SQS queue upon successful upload to s3
            queue.delete_message(

            )
        except  Exception as e:
            print(e)
            print('ERROR! Key: {} Bucket: {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
            raise e

# Converts video segment
def concat():
    if key is not None:
        file = open('/tmp/targetlist.txt', w)
        for each in sorted(os.listdir('/tmp/*.ts')):
            file.write(each)
                #writes ordered list of transport streams to file
        print "Concatenating video..."
        ff = ffmpy.FFmpeg(
        executable='./ffmpeg/ffmpeg',
        inputs={'/tmp/targetlist.txt' : '-f concat -safe 0'},
        outputs={'/tmp/'+key : '-y -c copy -bsf:a aac_adtstoasc'}
        )
        ff.run()
