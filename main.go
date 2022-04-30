package main

import (
	"context"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	presignExpirationMinutes = 5
)

var (
	logger        *zap.Logger
	listenAddress string
	s3BucketName  string

	s3Cache *S3ObjectStorageCache

	upstreamUrl   *url.URL
	preserveHost  bool
	useLocalStack bool
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

func readConfigFromEnvs() {
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
		upstreamRawUrl = "http://localhost:7000"
	}

	preserveHost = os.Getenv("BLOBLO_PRESERVE_HOST") == "true"

	var err error
	upstreamUrl, err = url.Parse(upstreamRawUrl)
	if err != nil {
		logger.Fatal("Can't parse the upstream url", zap.String("error", err.Error()))
	}

	useLocalStack = os.Getenv("BLOBLO_USE_LOCALSTACK") == "true"
}

func getLocalStackAwsConfig() (aws.Config, error) {
	localStackResolver := aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
		localstackUrl := "http://localhost:4566"

		return aws.Endpoint{
			PartitionID:   "aws",
			URL:           localstackUrl,
			SigningRegion: "us-east-1",
		}, nil
	})
	return config.LoadDefaultConfig(context.TODO(),
		config.WithEndpointResolver(localStackResolver))
}

func initS3Cache() {
	var awsConfig aws.Config
	var err error
	if useLocalStack {
		awsConfig, err = getLocalStackAwsConfig()
	} else {
		awsConfig, err = config.LoadDefaultConfig(context.TODO())
	}
	if err != nil {
		logger.Error("Error loading AWS connection configuration", zap.String("error", err.Error()))
		return
	}

	s3Client := s3.NewFromConfig(awsConfig, func(opts *s3.Options) {
		opts.UsePathStyle = true
	})
	_, err = s3Client.GetBucketLocation(context.TODO(), &s3.GetBucketLocationInput{Bucket: &s3BucketName})
	if err != nil {
		logger.Error("The AWS configuration seems to be invalid", zap.String("error", err.Error()))
		logger.Fatal(err.Error())
	}

	s3PresignClient := s3.NewPresignClient(s3Client)

	s3Cache = NewS3ObjectStorageCache(s3Client, s3PresignClient, s3BucketName, presignExpirationMinutes)
}

func main() {
	initLogger()
	defer logger.Sync()

	readConfigFromEnvs()
	initS3Cache()

	logger.Sugar().Infof("Hello, World! I will use %s as my upstream and listen on %s", upstreamUrl, listenAddress)
	logger.Sugar().Infof("I will keep my blobs in the bucket named %s", s3BucketName)
	logger.Info("Please keep your fingers crossed ;)")

	fallbackReverseProxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
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
	}

	blobloProxy := NewBlobloProxy(upstreamUrl, s3Cache, fallbackReverseProxy, logger)
	err := http.ListenAndServe(listenAddress, blobloProxy)
	if err != nil {
		logger.Fatal(err.Error())
	}
}
