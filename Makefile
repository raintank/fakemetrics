VERSION=$(shell git describe --tags --always | sed 's/^v//')

build:
	go build -ldflags "-X main.Version=$(VERSION)"

install:
	go install -ldflags "-X main.Version=$(VERSION)"
