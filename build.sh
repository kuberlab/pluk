#!/usr/bin/env bash

docker build --tag kuberlab/pluk:latest -f Dockerfile .
docker build --tag kuberlab/plukefs:latest -f Dockerfile.plukefs .

if [ "$1" == "--push" ];
then
    docker push kuberlab/pluk:latest
    docker push kuberlab/plukefs:latest
fi
