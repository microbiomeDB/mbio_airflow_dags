#!/bin/bash

fromDir=$1
fromFile=$2
toDir=$3
deleteAfter=$4
gzip=$5
sshTarget=$6

set -eo pipefail

# optionally gzip
gzipCmd=""
gunzipCmd=""
if [[ $gzip = true ]]; then
  gzipCmd="gzip -c |"
  gunzipCmd="gunzip -c |"
fi

sumFile="$fromFile.sum"

cd $toDir

# copy and checksum
remoteCmd="/bin/bash -c 'set -eo pipefail; cd $fromDir; tar cf - $fromFile | $gzipCmd tee >(md5sum > $sumFile)'"
localCmd="tee >(md5sum > $sumFile) | $gunzipCmd tar xf -"

/bin/bash -c "ssh -2 $sshTarget \"$remoteCmd\" | $localCmd"

checksumOnCluster=$(ssh -2 $sshTarget "cd $fromDir; cat $sumFile")
checksumLocal=$(cat $sumFile)

if [[ "$checksumOnCluster" != "$checksumLocal" ]]; then
    echo "Checksums do not match. Copy from cluster failed."
    exit 1
fi

# clean up
rm $sumFile
ssh -2 $sshTarget "cd $fromDir; rm -f $sumFile"

if [[ $deleteAfter = true ]]; then
    ssh -2 $sshTarget "cd $fromDir; rm -rf $fromFile"
fi

if [[ ! -f $toDir/$fromFile ]] && [[ ! -d $toDir/$fromFile ]]; then
    echo "File or directory did not successfully copy to $toDir/$fromFile"
    exit 1
fi
