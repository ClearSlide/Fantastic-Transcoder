import boto3, ffmpy, os, json

s3 = boto3.resource('s3')
dynamo = boto3.resource('dynamodb')
table = dynamo.Table('FT_SegmentState')
sqs = boto3.resource('sqs')

# Triggered by write to FT_VideoConversions
def lambda_handler(event, context):

    # Load triggering row from FT_VideoConversions and assign variables
    try:
        print event['Records'][0]['dynamodb']['NewImage']
        Row = event['Records'][0]['dynamodb']['NewImage']
        Bucket = Row['Bucket']['S']
        ConversionID = Row['ConversionID']['S']
        Filename, Extension = os.path.splitext(Row['Filename']['S'])
        Path = Row['Path']['S']
        StatusQueueMessageID = Row['QueueMessageID']['S']
        RequestedFormats = Row['RequestedFormats']['S']
    except KeyError:
        print "DynamoDB records are incomplete!"
    else:
        if Path == 'NULL':
            S3Path = '{}{}'.format(Filename, Extension)
        elif Path != 'NULL':
            S3Path = '{}{}{}'.format(Path, Filename, Extension)
        LocalPath = '/tmp/{}{}'.format(Filename, Extension)

        print 'Bucket/ConversionID is {}, {}'.format(Bucket, ConversionID)
        print 'StatusQueueMessageID is {}'.format(StatusQueueMessageID)

        '''
        statusqueue.send_message(
            MessageBody='Downloading source from S3...',
            MessageAttributes={
                'ConversionID': {
                    'StringValue': ConversionID,
                    'DataType': 'String'
                }
            }
        )'''
        try:
            print "Cleaning working directory"
            for f in os.listdir('/tmp/'):
                os.remove(f)
        except Exception as e:
            raise Exception('Failed to clean temp dir')
        try:
            print "Downloading original file..."
            s3.Bucket(Bucket).download_file(S3Path, LocalPath)
        except Exception as e:
            raise Exception('Failure downloading file from s3: {} to local: {} Exception: {}'.format(S3Path, LocalPath, e))
        '''
        statusqueue.send_message(
            MessageBody='Segmenting video...',
            MessageAttributes={
                'ConversionID': {
                    'StringValue': ConversionID,
                    'DataType': 'String'
                }
            }
        )'''
        # Segment video with ffmpeg
        segment(LocalPath)
        print "Removing original file..."
        os.remove(LocalPath)
        '''
        statusqueue.send_message(
            MessageBody='Uploading segments to S3',
            MessageAttributes={
                'ConversionID': {
                    'StringValue': ConversionID,
                    'DataType': 'String'
                }
            }
        )'''
        # Upload each segment to S3
        print 'Uploading segments and audio to s3...'
        for filename in os.listdir('/tmp/'):
            attempts = 0
            while attempts < 3:
                try:
                    print 'uploading {} to s3'.format(filename)
                    if Path != 'NULL':
                        s3.Bucket(Bucket).upload_file('/tmp/{}'.format(filename), '{}{}'.format(Path, filename))
                        break
                    elif Path == 'NULL':
                        s3.Bucket(Bucket).upload_file('/tmp/{}'.format(filename), '{}'.format(filename))
                        break
                except Exception as UploadError:
                    attempts += 1
                    raise Exception('Failure reuploading segment /tmp/{}, attempt {}'.format(filename, attempts))

            # Write to FT_SegmentState
            if filename.endswith('mp3'):
                SegmentID = '-1'
            else:
                segments = os.path.splitext(filename)[0].split('SEGMENT')
                SegmentID = segments[len(segments) - 1]
                print "preparing dynamo statement for SegmentID {}".format(SegmentID)
            writeattempts = 0
            while writeattempts < 3:
                try:
                    print 'Writing information to DynamoDB...'
                    print "Bucket: {} ConversionID: {} Filename: {} Path: {} StatusQueueMessageID: {} RequestedFormats: {} SegmentID: {}".format(Bucket, ConversionID, filename, Path, StatusQueueMessageID, RequestedFormats, SegmentID)
                    response = table.put_item(
                                    Item = {
                                        'Bucket': Bucket,
                                        'ConversionID': ConversionID,
                                        'Completed': 0,
                                        'Filename': filename,
                                        'Path': Path,
                                        'QueueMessageID': StatusQueueMessageID,
                                        'RequestedFormats': RequestedFormats,
                                        'SegmentID': SegmentID,
                                    }
                                )
                    formattedresponse = json.dumps(response, indent=4)
                    print('PutItem succeeded: {}'.format(formattedresponse))
                    break
                except Exception as DynamoError:
                    raise Exception('Failure writing data to dynamodb for segment {}, attempt {} Exception: {}'.format(SegmentID, attempts, DynamoError))
                    writeattempts += 1

# ffmpy invocation that SEGMENTs the video into chunks
def segment(path):
    if path is not None:
        try:
            print "Segmenting video file..."
            FilePath, Extension = os.path.splitext(path)
            f = ffmpy.FFmpeg(
                    executable='./ffmpeg/ffmpeg',
                    inputs={path : None},
                    outputs={'{}.mp3'.format(FilePath): '-c:a mp3'})
            ff = ffmpy.FFmpeg(
                    executable='./ffmpeg/ffmpeg',
                    inputs={path : None},
                    outputs={'{}SEGMENT%d{}'.format(FilePath, Extension): '-acodec copy -f segment -vcodec copy -reset_timestamps 1 -map 0'})
            f.run()
            ff.run()
            print "Segmentation complete"
        except Exception as ffmpegerror:
            raise Exception('Failure during segmentation of video file')
