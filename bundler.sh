#!/bin/sh

# To be run on an amazon linux box. Creates python virtualenv, installs ffmpeg, and bundles the lambda functions.

## Install ffmpeg
yum update
yum -y install autoconf automake build-essential git-core libass-devel.x86_64 libgpac-dev libsdl1.2-dev libtheora-dev libtool libx264-dev libvdpau-dev libvorbis-dev libx11-dev libxext-dev libxfixes-dev pkg-config texi2html zlib1g-dev libmp3lame-dev nasm gcc yasm && true

mkdir ~/ffmpeg_sources

# Build fdk-aac for audio encoding
cd ~/ffmpeg_sources
git clone --depth 1 git://github.com/mstorsjo/fdk-aac.git
cd fdk-aac
autoreconf -fiv
./configure --prefix="$HOME/ffmpeg_build" --disable-shared
make
make install
make distclean

# Build lame to handle WAV conversion
cd ~/ffmpeg_sources
wget http://downloads.sourceforge.net/project/lame/lame/3.99/lame-3.99.5.tar.gz
tar xzvf lame-3.99.5.tar.gz
cd lame-3.99.5
./configure --prefix="$HOME/ffmpeg_build" --enable-nasm --disable-shared
make
make install
make distclean

# Build ffmpeg!
cd ~/ffmpeg_sources
git clone --depth 1 git://source.ffmpeg.org/ffmpeg
cd ffmpeg
PKG_CONFIG_PATH="$HOME/ffmpeg_build/lib/pkgconfig"
export PKG_CONFIG_PATH
./configure --prefix="$HOME/ffmpeg_build" \
            --extra-cflags="-I$HOME/ffmpeg_build/include" --extra-ldflags="-L$HOME/ffmpeg_build/lib" \
            --bindir="$HOME/bin" --extra-libs="-ldl" --enable-gpl --enable-libass --enable-libfdk-aac \
            --enable-libmp3lame --enable-libx264 --enable-nonfree
make
make install
cp ffmpeg /usr/bin/
make distclean
hash -r
ffmpeg 2>&1 | head -n1

# Bundle lambda packages
rm ./poll.zip
zip -9 poll.zip poll.py
zip -r9 poll.zip ffmpeg
cd env/lib/python2.7/site-packages/;zip -r9 ~/lambda/poll.zip *

rm ./segment.zip
zip -9 segment.zip segment.py
zip -r9 segment.zip ffmpeg
cd env/lib/python2.7/site-packages/;zip -r9 ~/lambda/segment.zip *

rm ./convert.zip
zip -9 convert.zip convert.py
zip -r9 convert.zip ffmpeg
cd env/lib/python2.7/site-packages/;zip -r9 ~/lambda/convert.zip *

rm ./concat.zip
zip -9 concat.zip concat.py
zip -r9 concat.zip ffmpeg
cd env/lib/python2.7/site-packages/;zip -r9 ~/lambda/concat.zip *
