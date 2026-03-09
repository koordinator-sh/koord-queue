# syntax=docker/dockerfile:1.0-experimental

FROM --platform=$BUILDPLATFORM golang:1.24-alpine as build
ARG TARGETOS 
ARG TARGETARCH
ADD . /go/src/github.com/koord-queue
WORKDIR /go/src/github.com/koord-queue

RUN GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} CGO_ENABLED=0 go build -ldflags '-w' -o bin/koord-queue cmd/main.go	

FROM --platform=$TARGETPLATFORM registry.cn-hangzhou.aliyuncs.com/acs/alpine:3.16-update
COPY --from=build /go/src/github.com/koord-queue/bin/koord-queue /usr/bin/koord-queue
RUN chmod +x /usr/bin/koord-queue
ENTRYPOINT ["/usr/bin/koord-queue"]