.PHONY: build install cpu mem fmt vet lint

default: build

all: build cover install lint

build:
	go build

install:
	go install

check: build
	go test -race

shortcheck: build
	go test -test.short -run ^Test

bench: build
	go test -cpuprofile cpu.out -memprofile mem.out -bench .

cpu:
	go tool pprof -top cpu.out # Run 'make bench' to generate profile.

mem:
	go tool pprof -top mem.out # Run 'make bench' to generate profile.

gencover: build
	go test -coverprofile cover.out

cover: gencover
	go tool cover -func cover.out

showcover: gencover
	go tool cover -html cover.out

fmt:
	gofmt -w -s ./*.go

vet:
	go vet

check-cyclo:
	gocyclo -over 15 .

check-ineffassign:
	ineffassign .

check-spell:
	misspell -error README.md Makefile *.go

lint:
	golint -set_exit_status -min_confidence 0.9

precommit: fmt cover vet check-cyclo check-ineffassign check-spell lint
