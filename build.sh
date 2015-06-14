# !/bin/sh
docker run --rm -v "$PWD":/go/src/github.com/netbrain/dlog -w /go/src/github.com/netbrain/dlog golang:1.4 make
