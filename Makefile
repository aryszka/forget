.PHONY: cpu mem

SOURCES = $(shell find . -name '*.go')

default: build

deps:
	go install github.com/fzipp/gocyclo/cmd/gocyclo@v0.3.1
	go install github.com/gordonklaus/ineffassign@latest
	go install github.com/client9/misspell/cmd/misspell@v0.3.4
	go install golang.org/x/lint/golint@latest

build: $(SOURCES)
	go build

install: $(SOURCES)
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

fmt: $(SOURCES)
	gofmt -w -s ./*.go

vet: $(SOURCES)
	go vet

check-cyclo: $(SOURCES)
	gocyclo -over 15 .

check-ineffassign: $(SOURCES)
	ineffassign .

check-spell: $(SOURCES) README.md Makefile
	misspell -error README.md Makefile *.go

lint: $(SOURCES)
	golint -set_exit_status -min_confidence 0.9

precommit: build check fmt cover vet check-cyclo check-ineffassign check-spell lint
	# ok
