#!/bin/sh

sudo docker-compose stop spiderkeeper
sudo docker-compose build spiderkeeper
sudo docker-compose up -d spiderkeeper