.PHONY: check

# The single gate every change must leave green: docs check plus the Go gate.
check:
	.agents/bin/check-docs.sh origin/main
	go vet ./...
	go test -race ./...
	go mod tidy -diff
	! gofmt -l . | grep .
