#!/usr/bin/env bash
DIRECTORY="test/"
for v in 1 2 3 4 5
do
    mkdir -p ${DIRECTORY}test$v
    touch ${DIRECTORY}test$v/file{1..50000}
    touch ${DIRECTORY}test$v/file{50001..100000}
    touch ${DIRECTORY}test$v/file{100001..150000}
    touch ${DIRECTORY}test$v/file{150001..200000}
    echo $v
done

TOTAL=$(find $DIRECTORY -type f -printf . | wc -c)
echo "Total files: " $TOTAL