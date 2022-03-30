package main

import (
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const (
	presignExpirationMinutes = 5
)

var (
	listenAddress string
	s3BucketName  string
	s3Svc         *s3.S3
	s3Uploader    *s3manager.Uploader

	upstreamUrl *url.URL

	cachedBlobDigests map[string]bool
)

func init() {
	listenAddress = os.Getenv("BLOBLO_LISTEN_ADDR")
	if listenAddress == "" {
		listenAddress = ":7777"
	}

	s3BucketName = os.Getenv("BLOBLO_S3_BUCKET_NAME")
	if s3BucketName == "" {
		s3BucketName = "sample-bucket"
	}

	upstreamRawUrl := os.Getenv("BLOBLO_UPSTREAM_URL")
	if upstreamRawUrl == "" {
		upstreamRawUrl = "http://localhost:6666"
	}

	var err error
	upstreamUrl, err = url.Parse(upstreamRawUrl)
	if err != nil {
		log.Panic("Can't parse the upstream url", err)
	}

	useLocalStack := os.Getenv("BLOBLO_USE_LOCALSTACK")

	var awsSession *session.Session
	if useLocalStack == "true" {
		localstackResolver := func(service, region string, opts ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
			localstackUrl := "http://localhost:4566"

			return endpoints.ResolvedEndpoint{
				URL:           localstackUrl,
				SigningRegion: "us-east-1",
			}, nil
		}

		s3ForcePathStyle := true
		awsSession = session.Must(session.NewSessionWithOptions(session.Options{
			Config: aws.Config{
				Region:           aws.String("us-east-1"),
				EndpointResolver: endpoints.ResolverFunc(localstackResolver),
				S3ForcePathStyle: &s3ForcePathStyle,
			},
			SharedConfigState: session.SharedConfigEnable,
		}))
	} else {
		awsSession = session.Must(session.NewSession())
	}

	s3Svc = s3.New(awsSession)
	s3Uploader = s3manager.NewUploader(awsSession)
}

func presignBlob(blobDigest string) string {
	req, _ := s3Svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(s3BucketName),
		Key:    aws.String(blobDigest),
	})
	urlStr, err := req.Presign(presignExpirationMinutes * time.Minute)

	if err != nil {
		log.Println("Failed to sign request", err)
	}

	return urlStr
}

func blobInCache(blobDigest string) bool {
	_, err := s3Svc.HeadObject(&s3.HeadObjectInput{Bucket: &s3BucketName, Key: &blobDigest})
	return err == nil
}

type blobloProxy struct {
	proxy *httputil.ReverseProxy
}

func getUpstreamRequest(req *http.Request) *http.Request {
	upstreamReq := req.Clone(req.Context())
	upstreamReq.RequestURI = ""
	upstreamReq.Host = upstreamUrl.Host
	upstreamReq.URL.Host = upstreamUrl.Host
	upstreamReq.URL.Scheme = upstreamUrl.Scheme
	return upstreamReq
}

func (rl *blobloProxy) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	log.Println(req.RequestURI, req.Method)

	pathElements := strings.Split(req.RequestURI, "/")
	if req.Method == http.MethodGet && len(pathElements) > 2 && pathElements[len(pathElements)-2] == "blobs" {
		blobDigest := pathElements[len(pathElements)-1]

		headReq := getUpstreamRequest(req)
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
				log.Println("Serving the blob", blobDigest, "from cache for user", user)
				http.Redirect(w, req, presignBlob(blobDigest), http.StatusFound)
				return
			} else { // upload the blob to cache and return the layer to the client
				upstreamReq := getUpstreamRequest(req)
				response, err := http.DefaultClient.Do(upstreamReq)
				if err != nil {
					log.Println("Failed to reach the upstream", err)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				teeReader := io.TeeReader(response.Body, w)

				log.Println("Uploading blob ", blobDigest, "to cache")
				s3Uploader.Upload(
					&s3manager.UploadInput{
						Bucket: &s3BucketName,
						Key:    &blobDigest,
						Body:   teeReader,
					})
				return
			}
		}
	}

	rl.proxy.ServeHTTP(w, req)
}

func main() {
	log.Println("Hello, World! I will use", upstreamUrl, "as my upstream and listen on", listenAddress)
	log.Println("I will keep my blobs in the bucket named", s3BucketName)
	log.Println("Please keep your fingers crossed ;)")

	//a custom Director is needed, as we have to set the host header
	r := blobloProxy{proxy: &httputil.ReverseProxy{Director: func(req *http.Request) {
		req.URL.Scheme = upstreamUrl.Scheme
		req.URL.Host = upstreamUrl.Host
		req.Host = upstreamUrl.Host
		if _, ok := req.Header["User-Agent"]; !ok {
			// explicitly disable User-Agent so it's not set to default value
			req.Header.Set("User-Agent", "")
		}
	}}}
	http.ListenAndServe(listenAddress, &r)
}
