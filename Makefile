mod:
	@go mod download

build: mod
	@CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o ./kafka-perfomance-test main.go

build_osx: mod
	@CGO_ENABLED=1 GOOS=darwin GOARCH=amd64 go build -o ./kafka-perfomance-test main.go