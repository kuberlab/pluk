#!/usr/bin/env bash

RED="\033[0;31m"
GREEN="\033[0;32m"
NC='\033[0m' # No color

has_ulimit=$(which ulimit)

if [ ! -z "$has_ulimit" ]
then
  limit=$(ulimit -n)
  ulimit -n 8192
fi

tmp=$(mktemp)
go test -v github.com/kuberlab/pluk/... | tee $tmp
exit_code=${PIPESTATUS[0]}

# echo "$out"
num=$(cat $tmp | grep RUN | wc -l)
rm $tmp

if [ $exit_code -eq 0 ]; then
  echo -e "${GREEN}Run $num tests\nTests passed${NC}"
else
  echo -e "${RED}Run $num tests\nTests failed${NC}"
  exit $exit_code
fi

if [ ! -z "$has_ulimit" ]
then
  ulimit -n $limit
fi
