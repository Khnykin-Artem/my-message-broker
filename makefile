BINARY=broker
CMD=./cmd/broker

build:
	go build -o bin/$(BINARY) $(CMD)

run: build
	./bin/$(BINARY)

test:
	go test -v ./...

clean:
	rm -rf bin/

.PHONY: build run test clean