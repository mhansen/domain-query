FROM golang:1.15 as gobuilder
WORKDIR /app
COPY go.* ./
RUN go mod download
COPY . ./
RUN go build -mod=readonly -a -v dump.go
ENTRYPOINT ["/app/dump"]
