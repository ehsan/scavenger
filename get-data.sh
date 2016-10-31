#!/bin/bash
mkdir -p crawl-data/CC-MAIN-2014-35/segments/1408500800168.29/warc/
mkdir -p crawl-data/CC-MAIN-2014-35/segments/1408500800168.29/wat/
mkdir -p crawl-data/CC-MAIN-2014-35/segments/1408500800168.29/wet/

ccfiles=(
     'crawl-data/CC-MAIN-2014-35/segments/1408500800168.29/warc/CC-MAIN-20140820021320-00000-ip-10-180-136-8.ec2.internal.warc.gz'
     'crawl-data/CC-MAIN-2014-35/segments/1408500800168.29/wat/CC-MAIN-20140820021320-00000-ip-10-180-136-8.ec2.internal.warc.wat.gz'
     'crawl-data/CC-MAIN-2014-35/segments/1408500800168.29/wet/CC-MAIN-20140820021320-00000-ip-10-180-136-8.ec2.internal.warc.wet.gz'
   );

for f in ${ccfiles[@]}
do
  mkdir -p `dirname $f`
  echo "Downloading `basename $f` ..."
  echo "---"
wget https://commoncrawl.s3.amazonaws.com/$f -O $f
done
