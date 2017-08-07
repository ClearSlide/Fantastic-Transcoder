import boto3, ffmpy, os
from natsort import natsorted

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
dynamo = boto3.resource('dynamodb')
table = dynamo.Table('FT_VideoConversions')
sqs = boto3.client('sqs')
queue = sqs.get_queue_by_name(QueueName='FT_convert_queue')
#statusqueue = sqs.get_queue_by_name(QueueName='FT_status_queue')

def lambda_handler(event, context):
    # This job is triggered by FT_ConversionState

    Row = event[0]['dynamodb']['NewImage']
    Bucket = Row['Bucket']
    ConversionID = Row['ConversionID']
    Filename, Extension = os.path.splitext(Row['Filename'])
    Path = Row['Path']
    S3Path = '{}{}{}'.format(Path, Filename, Extension)
    LocalPath = '/tmp/{}/'.format(ConversionID)

    os.makedirs('/tmp/{}'.format(ConversionID))

    try:
        print 'Downloading audio/video files...'
        segments = table.query(KeyConditionExpression=Key('ConversionID').eq(ConversionID))['Items']
        for segment in segments:
            # Assuming segments share the same path as their original file.
            name = segment['Filename']
            segPath = '{}{}'.format(Path, name)
            s3.Bucket(Bucket).download_file(segPath, '{}{}'.format(LocalPath, name))

        #sqs.put_message(
        #    QueueUrl=statusqueue
        #    ReceiptHandle=StatusReceipt
        #    status='Saving')

        # concat - this overwrites the S3Path file
        upload = concat(LocalPath)

        # upload to destination
        print 'Uploading completed file to s3...'
        s3_client.upload_file(upload, Bucket, Path)

        #sqs.put_message(
        #    QueueUrl=statusqueue
        #    ReceiptHandle=StatusReceipt
        #    status='Finished')
        # Delete message from SQS queue upon successful upload to s3
        #sqs.delete_message(
        #    QueueUrl=queue,
        #    ReceiptHandle=ReceiptHandle
        #)
    except Exception as e:
        raise Exception('Failure during concatenation for ConversionID {} in bucket {}!'.format(ConversionID, Bucket))

# Converts video segment, assumes path is a dir
def concat(path):
    if path is not None:
        file = open('{}targetlist.txt'.format(path), w)

        #writes ordered list of transport streams to file
        audio, name = '', ''
        files = natsorted(os.listdir(path), key=lambda y: y.lower())
        for stream in files:
            if stream.endswith('.ts'):
                file.write(each)
            else:
                audio = stream
                name = os.path.splitext(stream)[0]

        print 'Concatenating video...'
        ff = ffmpy.FFmpeg(
            executable='./ffmpeg/ffmpeg',
            inputs={'{}targetlist.txt'.format(path) : '-f concat -safe 0'},
            outputs={'{}DEAF.mp4'.format(path) : '-y -c copy -bsf:a aac_adtstoasc'}
            )
        ff.run()

        file_to_upload = '{}{}FANTASITCALLY_TRANSCODED.mp4'.format(path, name)

        fff=ffmpy.FFmpeg(
            executable='./ffmpeg/ffmpeg',
            inputs={
                '{}DEAF.mp4'.format(path) : [None],
                '{}{}'.format(path, audio) : [None]
            },
            outputs={file_to_upload : '-c:v copy -c:a aac -strict experimental'}
        )
        file.close()

        return file_to_upload
