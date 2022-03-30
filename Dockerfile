FROM golang:1.18-buster AS build

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY *.go ./

RUN go build -o /bloblo

FROM gcr.io/distroless/base-debian11

WORKDIR /

COPY --from=build /bloblo /bloblo

EXPOSE 7777

USER nonroot:nonroot

ENV BLOBLO_UPSTREAM_URL https://registry-1.docker.io

CMD ["/bloblo"]