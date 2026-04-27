BINARY=broker
CMD=./cmd/broker

build:
	go build -o bin/$(BINARY) $(CMD)

run: build
	./bin/$(BINARY) --config configs/config.yaml

test:
	go test -v ./...

clean:
	rm -rf bin/ data/

.PHONY: build run test clean