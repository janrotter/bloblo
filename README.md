# bloblo

Just a proof of concept at the moment, not suitable for real world usage.

The idea is to offload the docker layers traffic to s3 by redirecting the client to presigned urls, reverse proxying
the rest of the traffic.

A simple HEAD request is made to verify if the user has the necessary permissions to access the layer.

## Test scenario

Currently the test shows a redirect to a presigned url for a single layer (`sha256:dfcff6d93b39097b3e4f343e505e1af69ccc98d4122439edc882f1ab908f48cb`),
as the logic for populating the cache is missing.

In order to demo please:
- launch a docker registry (e.g. by using https://github.com/janrotter/nexus_playground)
- upload the testdockerimage to the registry
- launch the localstack using the docker-compose from the localstack folder
- prepopulate the localstack's s3 using the `init_localstack.sh` script
- update the `upstreamUrl` in hello.go
- export the environment variables for the localstack, as instructed here: https://docs.localstack.cloud/integrations/aws-cli/
- launch the `bloblo` with `go run .`
- configure docker client authentication (`bloblo` will pass the credentials to the upstream),
  e.g. with `docker login localhost:7777`

Now, when pulling the image through `bloblo` you should reveive the cached layer from
the localstack's s3, instead of the hosted docker repository.

References:

A talk describing a reverse proxy implementation in golang
https://www.youtube.com/watch?v=tWSmUsYLiE4


A reverse proxy implementation in the standard library
https://pkg.go.dev/net/http/httputil#ReverseProxy


How to upload a file to s3 in chunks in golang
https://stackoverflow.com/questions/34177137/stream-file-upload-to-aws-s3-using-go


https://stackoverflow.com/questions/25671305/golang-io-copy-twice-on-the-request-body

Docker http API
https://docs.docker.com/registry/spec/api/
