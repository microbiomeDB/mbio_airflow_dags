#!/bin/

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

# copy and checksum
# TODO use scp or something?
remoteCmd="/bin/bash -c \"set -eo pipefail; cd $fromDir; tar cf - $fromFile | $gzip tee >(md5sum > $sumFile)\""
localCmd="tee >(md5sum > $sumFile) | $gunzip tar xf -"
ssh -2 $sshTarget "$remoteCmd" | $localCmd

checksumOnCluster=$(ssh -2 $sshTarget "cd $fromDir; cat $sumFile")
checksumLocal=$(cat $sumFile)

if [ "$checksumOnCluster" != "$checksumLocal" ]; then
    echo "Checksums do not match. Copy from cluster failed."
    exit 1
fi

# clean up
rm $sumFile
ssh -2 $sshTarget "cd $fromDir; rm -f $sumFile"

if ($deleteAfter) {
    ssh -2 $ssh_target "cd $fromDir; rm -rf $fromFile"
}

if ( ! -f $toDir/$fromFile) {
    echo "File did not successfully copy to $toDir/$fromFile"
    exit 1
}
