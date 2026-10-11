.PHONY: check

# Gate until Go code lands: the docs check. When Go code lands, add
# go vet ./... / go test ./... / go mod tidy -diff / gofmt — the dispatcher
# repo's Makefile is the reference shape.
check:
	.agents/bin/check-docs.sh origin/main
