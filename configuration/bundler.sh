  #!/bin/sh

  # To be run on an amazon linux box. Creates python virtualenv, installs ffmpeg, and bundles the lambda functions.
  # Assumes you have cloned Fantastic Transcoder into the home folder of the root user.
  FTPATH="/root/Fantastic-Transcoder"

  cd $FTPATH
  virtualenv env
  source $FTPATH/env/bin/activate
  pip install ffmpy
  pip install boto3
  ## Install ffmpeg
  yum update
  yum -y install autoconf automake build-essential git-core libgpac-dev libsdl1.2-dev libtheora-dev libtool libx264-dev libvdpau-dev libvorbis-dev libx11-dev libxext-dev libxfixes-dev pkg-config texi2html zlib1g-dev libmp3lame-dev nasm gcc yasm && true
  #advanced subtitle support coming sometime
  #wget https://downloads.sourceforge.net/freetype/freetype-2.8.tar.bz2
  #wget https://github.com/libass/libass/releases/download/0.13.7/libass-0.13.7.tar.xz
  #wget https://www.fribidi.org/download/fribidi-0.19.7.tar.bz2
  #tar -xvf libass-0.13.7.tar.xz
  #tar -xvf freetype-2.8.tar.bz2
  #tar -xvf fribidi-0.19.7.tar.bz2

  mkdir $FTPATH/ffmpeg_sources

  # Build fdk-aac for audio encoding
  cd $FTPATH/ffmpeg_sources
  git clone --depth 1 git://github.com/mstorsjo/fdk-aac.git
  cd fdk-aac
  autoreconf -fiv
  ./configure --prefix="$FTPATH/ffmpeg_build" --disable-shared
  make
  make install
  make distclean

  # Build lame to handle WAV conversion
  cd $FTPATH/ffmpeg_sources
  wget http://downloads.sourceforge.net/project/lame/lame/3.99/lame-3.99.5.tar.gz
  tar xzvf lame-3.99.5.tar.gz
  cd lame-3.99.5
  ./configure --prefix="$FTPATH/ffmpeg_build" --enable-nasm --disable-shared
  make
  make install
  make distclean

  # Build ffmpeg!
  cd $FTPATH/ffmpeg_sources
  git clone --depth 1 git://source.ffmpeg.org/ffmpeg
  cd ffmpeg
  PKG_CONFIG_PATH="$FTPATH/ffmpeg_build/lib/pkgconfig"
  export PKG_CONFIG_PATH
  ./configure --prefix="$FTPATH/ffmpeg_build" \
              --extra-cflags="-I$FTPATH/ffmpeg_build/include" --extra-ldflags="-L$FTPATH/ffmpeg_build/lib" \
              --bindir="$FTPATH/bin" --extra-libs="-ldl" --enable-gpl --enable-libass --enable-libfdk-aac \
              --enable-libmp3lame --enable-libx264 --enable-nonfree
  make
  make install
  cp -r $FTPATH/ffmpeg_sources/ffmpeg $FTPATH
  make distclean
  hash -r
  ffmpeg 2>&1 | head -n1

  # Bundle lambda packages
  rm -f $FTPATH/poll.zip
  cd $FTPATH;zip -9 poll.zip poll.py
  cd $FTPATH/env/lib/python2.7/site-packages/;zip -r9 $FTPATH/poll.zip ./*

  rm -f $FTPATH/segment.zip
  cd $FTPATH;zip -9 segment.zip segment.py
  cd $FTPATH;zip -r9 segment.zip ffmpeg
  cd $FTPATH/env/lib/python2.7/site-packages;zip -r9 $FTPATH/segment.zip ./*

  rm -f $FTPATH/convert.zip
  cd $FTPATH;zip -9 convert.zip convert.py
  cd $FTPATH;zip -r9 convert.zip ffmpeg
  cd $FTPATH/env/lib/python2.7/site-packages;zip -r9 $FTPATH/convert.zip ./*

  rm -f $FTPATH/concat.zip
  cd $FTPATH;zip -9 concat.zip concat.py
  cd $FTPATH;zip -r9 concat.zip ffmpeg
  cd $FTPATH/env/lib/python2.7/site-packages;zip -r9 $FTPATH/concat.zip ./*
