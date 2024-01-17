FROM golang:1.16.3-alpine3.12 as builder
RUN mkdir /build 
ADD . /build/
WORKDIR /build
RUN apk add --no-cache git bash
RUN scripts/build
FROM alpine
COPY --from=builder /build/cmd/blobby/blobby /app/
WORKDIR /app
CMD ["/app/blobby"]
EXPOSE 2001-2003
