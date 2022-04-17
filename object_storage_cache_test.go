package main

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
)

type s3ClientImpl struct {
	err error
}

func (s3Client *s3ClientImpl) HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return nil, s3Client.err
}

func (s3Client *s3ClientImpl) PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return nil, nil
}

func (s3Client *s3ClientImpl) UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	return nil, nil
}

func (s3Client *s3ClientImpl) CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	return nil, nil
}

func (s3Client *s3ClientImpl) CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	return nil, nil
}

func (s3Client *s3ClientImpl) AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	return nil, nil
}

func TestIsInCacheReturnsNilErrorForNotFoundItemsToAvoidFloodingLogs(t *testing.T) {
	s3Client := &s3ClientImpl{err: &smithy.GenericAPIError{Code: "NotFound"}}
	s3Cache := NewS3ObjectStorageCache(s3Client, nil, "bucketname", 5)
	isInCache, err := s3Cache.isBlobInCache("someobject")

	assert.Nil(t, err)
	assert.Equal(t, false, isInCache)
}
