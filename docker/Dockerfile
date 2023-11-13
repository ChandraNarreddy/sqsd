FROM golang:latest
WORKDIR /
ADD ./ ./
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./sqsd.cmd
RUN chmod +x sqsd.cmd