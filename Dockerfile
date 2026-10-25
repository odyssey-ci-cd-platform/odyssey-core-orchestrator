# Build stage: go.mod pins the toolchain floor (1.26.10, gh-23); a newer
# image Go compiles it directly, an older one auto-downloads it.
FROM golang:1.26.10-alpine AS build
WORKDIR /src
ENV GOTOOLCHAIN=auto CGO_ENABLED=0
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -trimpath -ldflags="-s -w" -o /out/orchestrator ./cmd/orchestrator

# Run stage: distroless static — no shell, no package manager, nonroot.
# The binary is pure Go (CGO off) and reads only env vars.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/orchestrator /orchestrator
ENTRYPOINT ["/orchestrator"]
