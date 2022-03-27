package main

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	listenAddress string
	s3BucketName  string
	s3Svc         *s3.S3

	upstreamUrl *url.URL

	cachedBlobDigests map[string]bool
)

func init() {
	listenAddress = ":7777"
	s3BucketName = "sample-bucket"
	upstreamUrl, _ = url.Parse("http://localhost:6666")

	cachedBlobDigests = make(map[string]bool)
	cachedBlobDigests["sha256:dfcff6d93b39097b3e4f343e505e1af69ccc98d4122439edc882f1ab908f48cb"] = true

	localstackResolver := func(service, region string, opts ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
		localstackUrl := "http://localhost:4566"

		return endpoints.ResolvedEndpoint{
			URL:           localstackUrl,
			SigningRegion: "us-east-1",
		}, nil
	}

	s3ForcePathStyle := true
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region:           aws.String("us-east-1"),
			EndpointResolver: endpoints.ResolverFunc(localstackResolver),
			S3ForcePathStyle: &s3ForcePathStyle,
		},
		SharedConfigState: session.SharedConfigEnable,
	}))

	s3Svc = s3.New(sess)
}

func presignBlob(blobDigest string) string {
	req, _ := s3Svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(s3BucketName),
		Key:    aws.String(blobDigest),
	})
	urlStr, err := req.Presign(15 * time.Minute)

	if err != nil {
		log.Println("Failed to sign request", err)
	}

	return urlStr
}

func blobInCache(blobDigest string) bool {
	return cachedBlobDigests[blobDigest]
}

type RequestLogger struct {
	proxy *httputil.ReverseProxy
}

func (rl *RequestLogger) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	log.Println(req.RequestURI, req.Method)

	pathElements := strings.Split(req.RequestURI, "/")
	if len(pathElements) > 2 && pathElements[len(pathElements)-2] == "blobs" {

		blobDigest := pathElements[len(pathElements)-1]

		headReq := req.Clone(req.Context())
		headReq.RequestURI = ""
		headReq.Host = upstreamUrl.Host
		headReq.URL.Host = upstreamUrl.Host
		headReq.URL.Scheme = upstreamUrl.Scheme
		headReq.Method = http.MethodHead
		response, err := http.DefaultClient.Do(headReq)
		if err != nil {
			log.Println("Failed to reach the upstream", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if response.StatusCode == http.StatusOK {
			if blobInCache(blobDigest) {
				user, _, _ := headReq.BasicAuth()
				log.Println("Serving the blob from cache for user", user)
				http.Redirect(w, req, presignBlob(blobDigest), http.StatusFound)
				return
			}
		}
	}

	rl.proxy.ServeHTTP(w, req)
}

func main() {
	fmt.Println("Hello, World! I will use", upstreamUrl, "as my upstream and listen on", listenAddress)

	proxy := httputil.NewSingleHostReverseProxy(upstreamUrl)
	r := new(RequestLogger)
	r.proxy = proxy
	http.ListenAndServe(listenAddress, r)
}
