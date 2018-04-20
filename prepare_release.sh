#!/bin/bash

version=$1

if [ -z $version ];
then
  echo "Usage: prepare_release.sh <version>"
  exit 1
fi

GOOS=linux go build -v -ldflags="-s -w -X main.VersionStr=$version" -o kdataset ./cmd/kdataset
cp kdataset kdataset-linux
tar -cvzf kdataset-linux-amd64-$version.tar.gz kdataset README.md

GOOS=darwin go build -v -ldflags="-s -w -X main.VersionStr=$version" -o kdataset ./cmd/kdataset
cp kdataset kdataset-osx
tar -cvzf kdataset-darwin-amd64-$version.tar.gz kdataset README.md

GOOS=windows go build -v -ldflags="-s -w -X main.VersionStr=$version" -o kdataset.exe ./cmd/kdataset
tar -cvzf kdataset-windows-amd64-$version.tar.gz kdataset.exe README.md

rm kdataset kdataset.exe

