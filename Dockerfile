FROM golang:1.15 as gobuilder
WORKDIR /app
COPY go.* ./
RUN go mod download
COPY . ./
RUN go build -mod=readonly -a -v dump.go

FROM alpine:3
RUN apk add --no-cache ca-certificates
COPY --from=gobuilder /app/dump /dump
ENTRYPOINT ["/dump"]
