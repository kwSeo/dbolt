.PHONY: all clean build-image run-simple-replica

clean:
	@echo 'Cleaning...'
	rm -rf dbolt-linux-amd64 simple-replica-linux-amd64

dbolt-server-linux-amd64:
	GOOS=linux GOARCH=amd64 go build -o dbolt-server-linux-amd64 cmd/dbolt-server/main.go

simple-replica-linux-amd64:
	GOOS=linux GOARCH=amd64 go build -o simple-replica-linux-amd64 cmd/simple-replica/main.go

build-image: dbolt-server-linux-amd64 simple-replica-linux-amd64
	docker build -t kwseo.io/dbolt-server:latest -f cmd/dbolt-server/Dockerfile .
	docker build -t kwseo.io/simple-replica:latest -f cmd/simple-replica/Dockerfile .
