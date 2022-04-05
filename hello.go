package main

import (
	"context"
	"crypto"
	"encoding/hex"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	presignExpirationMinutes = 5
)

var (
	listenAddress   string
	s3BucketName    string
	s3Client        *s3.Client
	s3PresignClient *s3.PresignClient
	s3Uploader      *manager.Uploader

	upstreamUrl  *url.URL
	preserveHost bool

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
		upstreamRawUrl = "http://localhost:8080/"
	}

	preserveHost = os.Getenv("BLOBLO_PRESERVE_HOST") == "true"

	var err error
	upstreamUrl, err = url.Parse(upstreamRawUrl)
	if err != nil {
		log.Panic("Can't parse the upstream url", err)
	}

	useLocalStack := os.Getenv("BLOBLO_USE_LOCALSTACK")
	var awsConfig aws.Config
	if useLocalStack == "true" {
		localStackResolver := aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
			localstackUrl := "http://localhost:4566"

			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           localstackUrl,
				SigningRegion: "us-east-1",
			}, nil
		})
		awsConfig, err = config.LoadDefaultConfig(context.TODO(),
			config.WithEndpointResolver(localStackResolver))
	} else {
		awsConfig, err = config.LoadDefaultConfig(context.TODO())
	}
	if err != nil {
		log.Printf("error: %v", err)
		return
	}

	s3Client = s3.NewFromConfig(awsConfig, func(opts *s3.Options) {
		opts.UsePathStyle = true
	})

	s3PresignClient = s3.NewPresignClient(s3Client)

	s3Uploader = manager.NewUploader(s3Client)
}

func presignBlob(blobDigest string) string {
	urlStr, err := s3PresignClient.PresignGetObject(
		context.TODO(),
		&s3.GetObjectInput{
			Bucket: aws.String(s3BucketName),
			Key:    aws.String(blobDigest),
		}, s3.WithPresignExpires(presignExpirationMinutes*time.Minute))

	if err != nil {
		log.Println("Failed to sign request", err)
	}

	return urlStr.URL
}

func blobInCache(blobDigest string) bool {
	_, err := s3Client.HeadObject(context.TODO(), &s3.HeadObjectInput{Bucket: &s3BucketName, Key: &blobDigest})
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

	if req.Method == http.MethodGet && (strings.HasSuffix(req.RequestURI, ".jar") || strings.HasSuffix(req.RequestURI, ".tar.gz") || strings.HasSuffix(req.RequestURI, ".tgz")) {
		headReq := getUpstreamRequest(req)
		headReq.Method = http.MethodHead
		response, err := http.DefaultClient.Do(headReq)
		if err != nil {
			log.Println("Failed to reach the upstream for HEAD, falling back to proxying the request", err)
			rl.proxy.ServeHTTP(w, req)
			return
		}
		if response.StatusCode == http.StatusOK {
			if etag, ok := response.Header["Etag"]; ok && len(etag) > 0 && !strings.HasPrefix(etag[0], "W/") {
				log.Println("Found etag: ", etag, "for", req.RequestURI)
				digestInput := req.RequestURI + "\n" + etag[0]
				sha256 := crypto.SHA256.New()
				sha256.Write([]byte(digestInput))
				blobDigest := hex.EncodeToString(sha256.Sum(nil))

				if blobInCache(blobDigest) {
					log.Println("Serving the blob", blobDigest, "from cache")
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

					contentType := response.Header.Get("content-type")

					log.Println("Uploading blob ", blobDigest, "to cache")
					_, err = s3Uploader.Upload(
						context.TODO(),
						&s3.PutObjectInput{
							Bucket:            &s3BucketName,
							Key:               &blobDigest,
							Body:              teeReader,
							ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
							ContentType:       &contentType,
						})
					if err != nil {
						log.Println("Error uploading blob", blobDigest, " : ", err)
					}

					return
				}
			}
		}
	}

	rl.proxy.ServeHTTP(w, req)
}

func main() {
	log.Println("Hello, World! I will use", upstreamUrl, "as my upstream and listen on", listenAddress)
	log.Println("I will keep my blobs in the bucket named", s3BucketName)
	log.Println("Please keep your fingers crossed ;)")
	log.Println()

	_, err := s3Client.GetBucketLocation(context.TODO(), &s3.GetBucketLocationInput{Bucket: &s3BucketName})
	if err != nil {
		log.Println("The AWS configuration seems to be invalid:")
		log.Panic(err)
	}

	//a custom Director is needed, as we have to set the host header
	r := blobloProxy{proxy: &httputil.ReverseProxy{Director: func(req *http.Request) {
		req.URL.Scheme = upstreamUrl.Scheme
		req.URL.Host = upstreamUrl.Host
		if !preserveHost {
			req.Host = upstreamUrl.Host
		}
		if _, ok := req.Header["User-Agent"]; !ok {
			// explicitly disable User-Agent so it's not set to default value
			req.Header.Set("User-Agent", "")
		}
	}}}
	err = http.ListenAndServe(listenAddress, &r)
	if err != nil {
		log.Panic(err)
	}
}
