#!/bin/sh

## Any video file put in s3 bucket triggers lambda
# segment video by keyframe
echo "Segmenting video"
time0=$(date +%s)
time ffmpeg -i ./Original/INPUT.m4v -acodec copy -f segment -vcodec copy -reset_timestamps 1 -map 0 ./Segmented/SEGMENT%d.m4v
## PUT outputs files to s3 bucket
## for each in s3 bucket, trigger (multiple?) lambda
## lambda converts segments to desired codec(s) and places them in another s3 bucket.
# Detect how many video files there are
max=$(ls -l ./Segmented | wc -l)
nummax=$(expr $max - 1)
realmax=$(expr $max - 2)
echo "$nummax segments created. Converting segments individually."
echo "-------------- BEGIN SEGMENT CONVERSION --------------"
time1=$(date +%s)
for i in $(seq 0 $realmax) ; do time ffmpeg -i ./Segmented/SEGMENT$i.m4v ./Converted/CONVERTED$i.mp4 ; echo "part $i complete" ; done
# Individual mp4 files are transcoded to mpeg transport streams for lossless conversion
echo "Transcoding segments into transport streams."
for i in $(seq 0 $realmax) ; do time ffmpeg -i ./Converted/CONVERTED$i.mp4 -c copy -bsf:v h264_mp4toannexb -f mpegts ./Transport/intermediate_$i.ts; echo "part $i complete" ; done
## next lambda triggers when those are all done(?), concats the intermediate transcode streams
echo "-------------- END SEGMENT CONVERSION --------------"

time2=$(date +%s)
timediff1=$(expr $time2 - $time1)

# Delete old targetlist - this is done here instead of at the end so that a log is preserved in case of weirdness
rm -rf ./targetlist.txt
for each in $(ls ./Transport/intermediate*); do
      echo "file '$each'" >> targetlist.txt
done
# sort the list numerically
sort -t _ -k 2 -g targetlist.txt -o targetlist.txt

echo "Splicing transport streams."
ffmpeg -f concat -safe 0 -i targetlist.txt  -c copy -bsf:a aac_adtstoasc ./Spliced/OUTPUT.mp4

# TODO: final lambda splices audio, if LiveRecord?
time4=$(date +%s)
timetotal=$(expr $time4 - $time0)
paralleltime=$(expr $time4 - $time0 - $timediff1)
echo "Total execution time was $timetotal seconds. Total time of conversion if all videos were converted in parallel would be $paralleltime seconds."
echo "Cleaning up working directories."
rm -rf ./Transport/*
rm -rf ./Converted/*
rm -rf ./Segmented/*

