#!/bin/bash

fromDir=$1
fromFile=$2
toDir=$3
gzip=$4
sshTarget=$5

set -eo pipefail

# optionally gzip
gzipCmd=""
gunzipCmd=""
if [[ $gzip = true ]]; then
  gzipCmd="gzip -c |"
  gunzipCmd="gunzip -c |"
fi

sumFile="$fromFile.sum"

cd $fromDir
# check fromFile exists
if [[ ! -f $fromFile ]]; then
    echo "File $fromFile does not exist in $fromDir"
    exit 1
fi

# copy and checksum
# TODO can we switch to scp or something?
localCmd="tar cfh - $fromFile | $gzipCmd tee >(md5sum > $sumFile)"
remoteCmd="/bin/bash -c \"set -eo pipefail; cd $toDir; tee >(md5sum > $sumFile) | $gunzipCmd tar xf -\""

/bin/bash -c "$localCmd | ssh -2 $sshTarget '$remoteCmd'"

checksumOnCluster=$(ssh -2 $sshTarget "cd $toDir; cat $sumFile")
checksumLocal=$(cat $sumFile)

if [[ "$checksumOnCluster" != "$checksumLocal" ]]; then
    echo "Checksums do not match. Copy to cluster failed."
    exit 1
fi

# clean up
rm $sumFile
ssh -2 $sshTarget "cd $toDir; rm -f $sumFile"
