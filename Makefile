GOOS=${TARGETOS}
ifeq ($(GOOS),)
GOOS=$(shell uname -s | tr A-Z a-z)
endif
GOARCH=${TARGETARCH}
ifeq ($(GOARCH),)
GOARCH=$(subst x86_64,amd64,$(patsubst i%86,386,$(shell uname -m)))
endif
# COMMONENVVAR=GOOS=$(shell uname -s | tr A-Z a-z) GOARCH=$(subst x86_64,amd64,$(patsubst i%86,386,$(shell uname -m)))
BUILDENVVAR=CGO_ENABLED=0

.PHONY: all
all: build

.PHONY: build
build: build-queue

.PHONY: build-queue
build-queue: fixcodec
	GOOS=$(GOOS) GOARCH=$(GOARCH) $(BUILDENVVAR) go build -ldflags '-w' -o bin/koord-queue cmd/main.go	

.PHONY: fixcodec
fixcodec:
	hack/fix-codec-factory.sh

.PHONY: update-vendor
update-vendor:
	hack/update-vendor.sh

.PHONY: unit-test
unit-test: fixcodec update-vendor
	hack/unit-test.sh

.PHONY: clean
clean:
	rm -rf ./bin
