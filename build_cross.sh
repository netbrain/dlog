#!/bin/bash
# ./build_cross.sh windows i386
# docker pull golang:1.4-cross
docker run --rm -v "$PWD":/go/src/github.com/netbrain/dlog -w /go/src/github.com/netbrain/dlog -e GOOS=$1 -e GOARCH=$2 golang:1.4-cross make init build
