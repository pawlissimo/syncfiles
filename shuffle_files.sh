#!/usr/bin/env bash
DIRECTORY="test/"

touch ${DIRECTORY}test5/file{200001..250000}
rm ${DIRECTORY}test2/file{1..50000}
touch ${DIRECTORY}test1/file{1..50000}

echo "Added: 50.000 files"
echo "Modified: 50.000 files"
echo "Removed: 50.000 files"