.PHONY: build check precommit

default: build

all: build cover install lint

build:
	go build

install:
	go install

check: build
	go test -race

checkshort: build
	go test -test.short

bench: build
	go test -cpuprofile cpu.out -memprofile mem.out -bench .

gencover: build
	go test -coverprofile cover.out

cover: gencover
	go tool cover -func cover.out

showcover: gencover
	go tool cover -html cover.out

fmt:
	go fmt

vet:
	go vet

lint:
	golint -set_exit_status

precommit: fmt cover vet lint
