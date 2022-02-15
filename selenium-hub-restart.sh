#!/bin/sh

###########################
# restart selenium hub    #
###########################

docker-compose -f docker-compose.prod.yml stop chrome
docker-compose -f docker-compose.prod.yml stop selenium-hub
docker-compose -f docker-compose.prod.yml up -d selenium-hub
docker-compose -f docker-compose.prod.yml up -d chrome
docker-compose -f  docker-compose.prod.yml scale chrome=3
