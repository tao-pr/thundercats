#!/bin/bash

# Run full testsuite

echo "Initialising DynamoDB data storage ..."

echo "... Starting Docker"
docker-compose -f thundercats/src/test/resources/docker-compose.yml up -d
CID_DYDB=$(docker container ls | grep dynamodb | awk '{print $1}')
echo "... Running DynamoDB with container ID : ${CID_DYDB}"

echo "... Creating a table"

aws dynamodb create-table \
    --table-name Entry \
    --attribute-definitions \
        AttributeName=Item,AttributeType=S \
        AttributeName=Value,AttributeType=N \
        AttributeName=Note,AttributeType=S \
    --key-schema \
      AttributeName=Item,KeyType=HASH AttributeName=Value,KeyType=RANGE \
    --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1

echo "... Injecting sample records"
aws dynamodb put-item \
    --table-name Entry \
    --item \
        '{"Item": {"S": "First Entry"}, "Value": {"N": 0}, "Note": {"S": "Unknown"}}' \
        '{"Item": {"S": "Second Entry"}, "Value": {"N": 1}, "Note": {"S": "Unknown"}}' \
        '{"Item": {"S": "Third Entry"}, "Value": {"N": 2}, "Note": {"S": "Unknown"}}' \
        '{"Item": {"S": "Forth Entry"}, "Value": {"N": 0}, "Note": {"S": ""}}' \
    --return-consumed-capacity TOTAL

echo "[DONE]"


echo "Executing SBT test ..."
sbt test

echo "All tests executed"
echo "Tearing down ..."

docker container stop $CID_DYDB

