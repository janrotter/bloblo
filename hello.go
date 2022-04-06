package main

import (
	"context"
	"io"
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
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	presignExpirationMinutes = 5
)

var (
	logger          *zap.Logger
	listenAddress   string
	s3BucketName    string
	s3Client        *s3.Client
	s3PresignClient *s3.PresignClient
	s3Uploader      *manager.Uploader

	upstreamUrl  *url.URL
	preserveHost bool

	cachedBlobDigests map[string]bool
)

func initLogger() {

	infoLevel := zap.LevelEnablerFunc(func(level zapcore.Level) bool {
		return level == zapcore.InfoLevel
	})

	errorLevel := zap.LevelEnablerFunc(func(level zapcore.Level) bool {
		return level == zapcore.ErrorLevel || level == zapcore.FatalLevel || level == zapcore.PanicLevel
	})

	stdout := zapcore.Lock(os.Stdout)
	stderr := zapcore.Lock(os.Stderr)

	core := zapcore.NewTee(
		zapcore.NewCore(
			zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
			stdout,
			infoLevel,
		),
		zapcore.NewCore(
			zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
			stderr,
			errorLevel,
		),
	)

	logger = zap.New(core)
}

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

	preserveHost = os.Getenv("BLOBLO_PRESERVE_HOST") == "true"

	var err error
	upstreamUrl, err = url.Parse(upstreamRawUrl)
	if err != nil {
		logger.Fatal("Can't parse the upstream url", zap.String("error", err.Error()))
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
		logger.Error("Error loading AWS connection configuration", zap.String("error", err.Error()))
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
		logger.Error("Failed to sign request", zap.String("error", err.Error()))
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
	logger.Info("Incoming request", zap.String("request", req.RequestURI), zap.String("method", req.Method))

	pathElements := strings.Split(req.RequestURI, "/")
	if req.Method == http.MethodGet && len(pathElements) > 2 && pathElements[len(pathElements)-2] == "blobs" {
		blobDigest := pathElements[len(pathElements)-1]

		headReq := getUpstreamRequest(req)
		headReq.Method = http.MethodHead
		response, err := http.DefaultClient.Do(headReq)
		if err != nil {
			logger.Error("Failed to reach the upstream", zap.String("error", err.Error()))
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if response.StatusCode == http.StatusOK {
			if blobInCache(blobDigest) {
				user, _, _ := headReq.BasicAuth()
				logger.Info("Serving blob from cache", zap.String("digest", blobDigest), zap.String("user", user), zap.String("action", "serve_blob"))
				http.Redirect(w, req, presignBlob(blobDigest), http.StatusFound)
				return
			} else { // upload the blob to cache and return the layer to the client
				upstreamReq := getUpstreamRequest(req)
				response, err := http.DefaultClient.Do(upstreamReq)
				if err != nil {
					logger.Error("Failed to reach the upstream", zap.String("error", err.Error()))
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				teeReader := io.TeeReader(response.Body, w)

				logger.Info("Uploading blob to cache", zap.String("digest", blobDigest), zap.String("action", "upload_blob"))
				_, err = s3Uploader.Upload(
					context.TODO(),
					&s3.PutObjectInput{
						Bucket:            &s3BucketName,
						Key:               &blobDigest,
						Body:              teeReader,
						ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
					})
				if err != nil {
					logger.Error("Error uploading blob", zap.String("digest", blobDigest), zap.String("error", err.Error()))
				}

				return
			}
		}
	}

	rl.proxy.ServeHTTP(w, req)
}

func main() {
	initLogger()
	logger.Sugar().Infof("Hello, World! I will use %s as my upstream and listen on %s", upstreamUrl, listenAddress)
	logger.Sugar().Infof("I will keep my blobs in the bucket named %s", s3BucketName)
	logger.Info("Please keep your fingers crossed ;)")

	_, err := s3Client.GetBucketLocation(context.TODO(), &s3.GetBucketLocationInput{Bucket: &s3BucketName})
	if err != nil {
		logger.Error("The AWS configuration seems to be invalid", zap.String("error", err.Error()))
		logger.Fatal(err.Error())
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
	},
		ErrorLog: zap.NewStdLog(logger),
	}}
	err = http.ListenAndServe(listenAddress, &r)
	if err != nil {
		logger.Fatal(err.Error())
	}
}
