GOOS=${TARGETOS}
ifeq ($(GOOS),)
GOOS=$(shell uname -s | tr A-Z a-z)
endif
GOARCH=${TARGETARCH}
ifeq ($(GOARCH),)
GOARCH=$(subst x86_64,amd64,$(patsubst i%86,386,$(shell uname -m)))
endif
BUILDENVVAR=CGO_ENABLED=0 GOWORK=off

# ENVTEST_K8S_VERSION refers to the version of kubebuilder assets to be downloaded by envtest binary.
ENVTEST_K8S_VERSION = 1.33
ENVTEST_VERSION = 71f7db556ca57ce7ea6563f77d739f0d2a54233a
CONTROLLER_GEN_VERSION = v0.18.0

# Setting SHELL to bash allows bash commands to be executed by recipes.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

## Tool Binaries
ENVTEST ?= $(LOCALBIN)/setup-envtest

.PHONY: all
all: build

.PHONY: build
build: build-queue build-controllers

.PHONY: build-queue
build-queue: $(LOCALBIN)
	GOOS=$(GOOS) GOARCH=$(GOARCH) $(BUILDENVVAR) go build -mod=readonly -ldflags '-w' -o bin/koord-queue cmd/main.go

.PHONY: build-controllers
build-controllers: $(LOCALBIN)
	GOOS=$(GOOS) GOARCH=$(GOARCH) $(BUILDENVVAR) go build -mod=readonly -ldflags '-w' -o bin/koord-queue-controllers ./cmd/controllers

.PHONY: unit-test
unit-test:
	GOWORK=off hack/unit-test.sh

.PHONY: envtest
envtest: $(ENVTEST) ## Download envtest-setup locally if necessary.
$(ENVTEST): $(LOCALBIN)
	GOWORK=off GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-runtime/tools/setup-envtest@$(ENVTEST_VERSION)

.PHONY: setup-envtest
setup-envtest: envtest ## Download kubebuilder assets for envtest.
	$(ENVTEST) use $(ENVTEST_K8S_VERSION) -p path --bin-dir $(LOCALBIN)/k8s

.PHONY: integration-test
integration-test: ## Run integration tests with envtest.
	GOWORK=off ENVTEST=$(ENVTEST) ENVTEST_VERSION=$(ENVTEST_VERSION) ENVTEST_K8S_VERSION=$(ENVTEST_K8S_VERSION) hack/integration-test.sh

.PHONY: update-crd
update-crd:
	GOWORK=off CONTROLLER_GEN_VERSION=$(CONTROLLER_GEN_VERSION) hack/update-crd.sh

.PHONY: verify-crd
verify-crd:
	GOWORK=off CONTROLLER_GEN_VERSION=$(CONTROLLER_GEN_VERSION) hack/verify-crd.sh

.PHONY: verify-helm
verify-helm:
	hack/verify-helm.sh

.PHONY: clean
clean:
	rm -rf ./bin
