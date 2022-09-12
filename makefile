SHELL := /bin/bash

all:
	go build ./app/gtfs-loader
	go build ./app/gtfs-monitor
	go build ./app/model-mgr
	go build ./app/gtfs-aggregator
	go build ./app/gtfs-tripupdate-svc

run-loader:
	go run app/gtfs-loader/main.go load

run-loader-list:
	go run app/gtfs-loader/main.go list

run-loader-help:
	go run app/gtfs-loader/main.go -h

test:
	go test ./... -count=1

tidy:
	go mod tidy
	go mod vendor