#!/bin/bash
echo "$@"
printenv
cp -r testdata "$NIGHTSHIFT_MOUNT_PATH"
