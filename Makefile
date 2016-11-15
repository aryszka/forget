.PHONY: check

default: build

all: build cover install

build:
	go build

install:
	go install

check: build
	go test -coverprofile cover.out

checkshort: build
	go test -test.short

cover: check
	go tool cover -func cover.out

showcover: check
	go tool cover -html cover.out

fmt:
	go fmt
