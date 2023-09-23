.PHONY: all clean build-image run-simple-replica

clean:
	@echo 'Cleaning...'
	rm -rf dbolt-linux-amd64 simple-replica-linux-amd64

simple-replica-linux-amd64:
	GOOS=linux GOARCH=amd64 go build -o simple-replica-linux-amd64 cmd/simple-replica/main.go

build-image: simple-replica-linux-amd64
	docker build -t kwseo.io/simple-replica:latest -f cmd/simple-replica/Dockerfile .

