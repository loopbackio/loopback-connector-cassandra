#!/bin/bash

### Shell script to spin up a docker container for cassandra.

## color codes
RED='\033[1;31m'
GREEN='\033[1;32m'
YELLOW='\033[1;33m'
CYAN='\033[1;36m'
PLAIN='\033[0m'

## variables
CASSANDRA_CONTAINER="cassandra_c"
HOST="localhost"
PORT=9042
KEYSPACE="test"
if [ "$1" ]; then
    HOST=$1
fi
if [ "$2" ]; then
    PORT=$2
fi
if [ "$3" ]; then
    KEYSPACE=$3
fi

## check if docker exists
printf "\n${RED}>> Checking for docker${PLAIN} ${GREEN}...${PLAIN}"
docker -v > /dev/null 2>&1
DOCKER_EXISTS=$?
if [ "$DOCKER_EXISTS" -ne 0 ]; then
    printf "\n\n${CYAN}Status: ${PLAIN}${RED}Docker not found. Terminating setup.${PLAIN}\n\n"
    exit 1
fi
printf "\n${CYAN}Found docker. Moving on with the setup.${PLAIN}\n"


## cleaning up previous builds
printf "\n${RED}>> Finding old builds and cleaning up${PLAIN} ${GREEN}...${PLAIN}"
docker rm -f $CASSANDRA_CONTAINER > /dev/null 2>&1
printf "\n${CYAN}Clean up complete.${PLAIN}\n"

## pull latest cassandra image
printf "\n${RED}>> Pulling latest cassandra image${PLAIN} ${GREEN}...${PLAIN}"
docker pull cassandra:latest > /dev/null 2>&1
printf "\n${CYAN}Image successfully built.${PLAIN}\n"

## run the cassandra container
printf "\n${RED}>> Starting the cassandra container${PLAIN} ${GREEN}...${PLAIN}"
CONTAINER_STATUS=$(docker run --name $CASSANDRA_CONTAINER -p $PORT:9042 -d cassandra:latest 2>&1)
if [[ "$CONTAINER_STATUS" == *"Error"* ]]; then
    printf "\n\n${CYAN}Status: ${PLAIN}${RED}Error starting container. Terminating setup.${PLAIN}\n\n"
    exit 1
fi
printf "\n${CYAN}Container is up and running.${PLAIN}\n"

## export the schema to the cassandra database
printf "\n${RED}>> Creating keyspace${PLAIN} ${GREEN}...${PLAIN}\n"

## command to export schema
docker exec -it $CASSANDRA_CONTAINER cqlsh -e "CREATE KEYSPACE $KEYSPACE WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3};" > /dev/null 2>&1

## variables needed to health check export schema
OUTPUT=$?
TIMEOUT=120
TIME_PASSED=0
WAIT_STRING="."

printf "\n${GREEN}Waiting for database to respond $WAIT_STRING${PLAIN}"
while [ "$OUTPUT" -ne 0 ] && [ "$TIMEOUT" -gt 0 ]
    do
        docker exec -it $CASSANDRA_CONTAINER cqlsh -e "CREATE KEYSPACE $KEYSPACE WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3};" > /dev/null 2>&1
        OUTPUT=$?
        sleep 1s
        TIMEOUT=$((TIMEOUT - 1))
        TIME_PASSED=$((TIME_PASSED + 1))

        if [ "$TIME_PASSED" -eq 5 ]; then
            printf "${GREEN}.${PLAIN}"
            TIME_PASSED=0
        fi
    done

if [ "$TIMEOUT" -le 0 ]; then
    printf "\n\n${CYAN}Status: ${PLAIN}${RED}Failed to created keyspace. Terminating setup.${PLAIN}\n\n"
    exit 1
fi
printf "\n${CYAN}Successfully created keyspace.${PLAIN}\n"

## set env variables for running test
printf "\n${RED}>> Setting env variables to run test${PLAIN} ${GREEN}...${PLAIN}"
export CASSANDRA_HOST=$HOST
export CASSANDRA_PORT=$PORT
export CASSANDRA_KEYSPACE=$KEYSPACE
printf "\n${CYAN}Env variables set.${PLAIN}\n"

printf "\n${CYAN}Status: ${PLAIN}${GREEN}Set up completed successfully.${PLAIN}\n"
printf "\n${CYAN}To run the test suite:${PLAIN} ${YELLOW}npm test${PLAIN}\n\n"
