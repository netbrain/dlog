== DLog an experimental distributed event log persistence solution ==

```go get github.com/netbrain/dlog```

then start the server with `dlog`

clients should use the client package

Prerequisites:

Install docker then,

`docker pull golang:1.4 && docker pull golang:1.4-cross`

`run.sh` - builds and runs the application 

`build.sh`  - builds the application and creates executable

`build_cross.sh` - cross compiles executable, ex. `build_cross.sh windows 386`