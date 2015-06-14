all: test build

test:
	go get -v && go test ./... -v

build: clean
	go build -v -i

clean:
	rm -rvf dlog dlog.*
