#!/bin/bash

# Run full testsuite
echo "Initialising :"
echo " - DynamoDB data storage ..."
echo " - Kafka ..."

echo "... Starting Docker"
docker-compose -f thundercats/src/test/resources/docker-compose.yml up -d

CID_DYDB=$(docker container ls | grep "amazon/dynamodb-local" | awk '{print $1}')
CID_KFK=$(docker container ls | grep "confluentinc/cp-kafka" | awk '{print $1}')
CID_ZKP=$(docker container ls | grep "confluentinc/cp-zookeeper" | awk '{print $1}')

echo "... Running DynamoDB with container ID  : ${CID_DYDB}"
echo "... Running Zookeeper with container ID : ${CID_ZKP}"
echo "... Running Kafka with container ID     : ${CID_KFK}"
echo
echo "... Creating a DynamoDB table"

aws dynamodb create-table \
    --table-name Entry \
    --attribute-definitions \
        AttributeName=Item,AttributeType=S \
        AttributeName=Value,AttributeType=N \
    --key-schema \
      AttributeName=Item,KeyType=HASH \
      AttributeName=Value,KeyType=RANGE \
    --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1 \
    --endpoint-url http://localhost:8000 > /dev/null

echo "... Injecting sample DynamoDB records"
aws dynamodb put-item \
    --table-name Entry \
    --item '{"Item": {"S": "First Entry"}, "Value": {"N": "0"}, "Note": {"S": "Unknown"}}' \
    --return-consumed-capacity TOTAL \
    --endpoint-url http://localhost:8000 > /dev/null

aws dynamodb put-item \
    --table-name Entry \
    --item '{"Item": {"S": "Second Entry"}, "Value": {"N": "1"}, "Note": {"S": "Unknown"}}' \
    --return-consumed-capacity TOTAL \
    --endpoint-url http://localhost:8000 > /dev/null

aws dynamodb put-item \
    --table-name Entry \
    --item '{"Item": {"S": "Third Entry"}, "Value": {"N": "2"}, "Note": {"S": "Unknown"}}' \
    --return-consumed-capacity TOTAL \
    --endpoint-url http://localhost:8000 > /dev/null

aws dynamodb put-item \
    --table-name Entry \
    --item '{"Item": {"S": "Forth Entry"}, "Value": {"N": "0"}, "Note": {"S": ""}}' \
    --return-consumed-capacity TOTAL \
    --endpoint-url http://localhost:8000 > /dev/null  

echo "[DONE]"


echo "Executing SBT test ..."
sbt test

echo "All tests executed"
echo "Tearing down ..."

docker container stop $CID_DYDB
docker container stop $CID_KFK
docker container stop $CID_ZKP

echo "[DONE]"

