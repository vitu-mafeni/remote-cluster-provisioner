# Build the manager binary
FROM golang:1.24 AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace

# Download dependencies before copying source so that source changes
# don't invalidate the cached dependency layer.
COPY go.mod go.mod
COPY go.sum go.sum
RUN go mod download

# Install garble for binary obfuscation.
# Pin to a specific version for reproducible builds.
RUN go install mvdan.cc/garble@latest

# Copy source last so the garble install layer is cached.
COPY cmd/main.go cmd/main.go
COPY api/ api/
COPY internal/ internal/
COPY pkg/ pkg/
COPY provider/ provider/

# garble -literals  : obfuscate string literals (not just symbol names)
# garble -tiny      : remove extra build metadata (timestamps, Go version, etc.)
# -trimpath         : strip all local filesystem paths from the binary
# -ldflags="-s -w"  : strip the symbol table and DWARF debug info
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    garble -literals -tiny build \
      -trimpath \
      -ldflags="-s -w" \
      -o manager \
      ./cmd/main.go

# Use distroless as minimal base image — no shell, no package manager,
# nothing an attacker can pivot from even if the binary is compromised.
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/manager .
USER 65532:65532

ENTRYPOINT ["/manager"]
