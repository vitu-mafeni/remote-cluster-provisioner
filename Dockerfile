# Build the manager binary
#
# go.mod is the single source of truth for the Go version: CI (test/lint/e2e and
# the publish gate) uses `go-version-file: go.mod`, so the image must be built
# with the same toolchain, otherwise what is tested is not what ships.
# GO_VERSION below must equal the `toolchain` (or, if absent, `go`) line of
# go.mod; the publish workflow enforces that and passes it as --build-arg.
#
# Base images are pinned by tag AND digest (the digest is what is actually
# pulled; the tag documents intent). When bumping go.mod, refresh GO_VERSION and
# GO_IMAGE_DIGEST together, e.g.:
#   docker buildx imagetools inspect golang:1.24.5 | head -3
# Set --build-arg GO_IMAGE_DIGEST= (empty) to build against the floating tag, or
# override GO_IMAGE / RUNTIME_IMAGE entirely.
ARG GO_VERSION=1.24.5
# Multi-arch index digest of golang:${GO_VERSION} (valid for the default above only).
ARG GO_IMAGE_DIGEST=sha256:ef5b4be1f94b36c90385abd9b6b4f201723ae28e71acacb76d00687333c17282
ARG GO_IMAGE=golang:${GO_VERSION}${GO_IMAGE_DIGEST:+@${GO_IMAGE_DIGEST}}
ARG RUNTIME_IMAGE=gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab

FROM ${GO_IMAGE} AS builder
ARG TARGETOS
ARG TARGETARCH
# Binary obfuscation with garble is OPT-IN (default: off). `-tiny` strips
# panic/stack information and `-literals` rewrites string literals, which makes
# crashes much harder to debug and has not been verified against this
# controller's reflection-based scheme/CRD registration. Enable it only after
# testing the resulting image:  docker build --build-arg OBFUSCATE=true ...
ARG OBFUSCATE=false

WORKDIR /workspace

# Download dependencies before copying source so that source changes
# don't invalidate the cached dependency layer.
COPY go.mod go.mod
COPY go.sum go.sum
RUN go mod download

# Install garble only when obfuscation is requested.
# Pinned to a specific version for reproducible, cacheable builds.
RUN if [ "${OBFUSCATE}" = "true" ]; then go install mvdan.cc/garble@v0.17.0; fi

# Copy source last so the dependency/garble layers are cached.
COPY cmd/main.go cmd/main.go
COPY api/ api/
COPY internal/ internal/
COPY pkg/ pkg/
COPY provider/ provider/

# -trimpath         : strip all local filesystem paths from the binary
# -ldflags="-s -w"  : strip the symbol table and DWARF debug info
# With OBFUSCATE=true additionally:
#   garble -literals : obfuscate string literals (not just symbol names)
#   garble -tiny     : remove extra build metadata and panic/stack details
RUN if [ "${OBFUSCATE}" = "true" ]; then BUILD="garble -literals -tiny build"; else BUILD="go build"; fi && \
    CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    $BUILD \
      -trimpath \
      -ldflags="-s -w" \
      -o manager \
      ./cmd/main.go

# Use distroless as minimal base image — no shell, no package manager,
# nothing an attacker can pivot from even if the binary is compromised.
FROM ${RUNTIME_IMAGE}
WORKDIR /
COPY --from=builder /workspace/manager .
USER 65532:65532

ENTRYPOINT ["/manager"]
