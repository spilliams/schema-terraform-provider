.PHONY: build
build: bin/tree

.PHONY: tidy
tidy:
	go mod tidy

bin/tree:
	go build -o bin/tree example/main.go
