package main

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

type ObjectStorageCache interface {
	getPresignedUrl(blobDigest string) (string, error)
	isBlobInCache(blobDigest string) (bool, error)
	uploadBlob(blobDigest string, body io.Reader) error
}

type S3ObjectStorageCache struct {
	s3BucketName             string
	s3Client                 s3.HeadObjectAPIClient
	s3PresignClient          S3PresignGetObjectAPIClient
	s3Uploader               *manager.Uploader
	presignExpirationMinutes int
}

type S3ClientInterface interface {
	s3.HeadObjectAPIClient
	manager.UploadAPIClient
}

type S3PresignGetObjectAPIClient interface {
	PresignGetObject(
		ctx context.Context,
		params *s3.GetObjectInput,
		optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error)
}

func NewS3ObjectStorageCache(s3Client S3ClientInterface, s3PresignClient S3PresignGetObjectAPIClient, bucketName string, presignExpirationMinutes int) *S3ObjectStorageCache {
	return &S3ObjectStorageCache{
		s3BucketName:             bucketName,
		s3Client:                 s3Client,
		s3PresignClient:          s3PresignClient,
		s3Uploader:               manager.NewUploader(s3Client),
		presignExpirationMinutes: presignExpirationMinutes,
	}
}

func (s3Cache *S3ObjectStorageCache) getPresignedUrl(blobDigest string) (string, error) {
	urlStr, err := s3Cache.s3PresignClient.PresignGetObject(
		context.TODO(),
		&s3.GetObjectInput{
			Bucket: aws.String(s3Cache.s3BucketName),
			Key:    aws.String(blobDigest),
		}, s3.WithPresignExpires(time.Duration(s3Cache.presignExpirationMinutes)*time.Minute))

	if err != nil {
		return "", err
	}

	return urlStr.URL, nil
}

func (s3Cache *S3ObjectStorageCache) isBlobInCache(blobDigest string) (isInCache bool, err error) {
	_, err = s3Cache.s3Client.HeadObject(context.TODO(), &s3.HeadObjectInput{Bucket: &s3Cache.s3BucketName, Key: &blobDigest})

	if err != nil {
		isInCache = false

		var ae smithy.APIError
		if errors.As(err, &ae) && ae.ErrorCode() == "NotFound" {
			err = nil
		}
	} else {
		isInCache = true
	}
	return
}

func (s3Cache *S3ObjectStorageCache) uploadBlob(blobDigest string, body io.Reader) error {
	_, err := s3Cache.s3Uploader.Upload(
		context.TODO(),
		&s3.PutObjectInput{
			Bucket:            &s3Cache.s3BucketName,
			Key:               &blobDigest,
			Body:              body,
			ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
		})

	return err
}

// Make sure S3ObjectStorageCache implements the ObjectStorageCache interface
var _ ObjectStorageCache = (*S3ObjectStorageCache)(nil)
