FROM golang:1.21.2
WORKDIR /
ADD ./ ./
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./zensqsd.cmd
RUN chmod +x zensqsd.cmd