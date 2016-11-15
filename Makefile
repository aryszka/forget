.PHONY: build check precommit

default: build

all: build cover install lint

build:
	go build

install:
	go install

check: build
	go test -race -coverprofile cover.out

checkshort: build
	go test -test.short

bench: build
	go test -cpuprofile cpu.out -memprofile mem.out -bench .

cover: check
	go tool cover -func cover.out

showcover: check
	go tool cover -html cover.out

fmt:
	go fmt

vet:
	go vet

lint:
	golint -set_exit_status

precommit: fmt cover vet lint
