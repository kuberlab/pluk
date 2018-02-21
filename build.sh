#!/usr/bin/env bash

docker build --tag kuberlab/pluk:latest -f Dockerfile .
docker build --tag kuberlab/plukefs:latest -f Dockerfile.plukfs .
