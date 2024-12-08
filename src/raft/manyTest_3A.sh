#!/bin/bash

count=0
success_count=0
fail_count=0

max_tests=500

for ((i=1; i<=max_tests; i++))
do
    echo "Running test iteration $i of $max_tests..."

    go test -v -run 3A &> output.log

    if [ "$?" -eq 0 ]; then
        success_count=$((success_count+1))
        echo "Test iteration $i passed."
    else
        fail_count=$((fail_count+1))
        exit 1
    fi
done

exit 0
