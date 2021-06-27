SHELL := /bin/bash

run:
	go run app/gtfs-loader/main.go load

run-list:
	go run app/gtfs-loader/main.go list

run-help:
	go run app/gtfs-loader/main.go -h

tidy:
	go mod tidy
	go mod vendor