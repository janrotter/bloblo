package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type testObjectStorageCache struct {
	checkedForBlob           bool
	uploadedBlob             bool
	redirectedToPresignedUrl bool
}

func (cache *testObjectStorageCache) getPresignedUrl(blobDigest string) (string, error) {
	cache.redirectedToPresignedUrl = true
	return "http://localtest.me/a_presigned_url", nil
}

func (cache *testObjectStorageCache) isBlobInCache(blobDigest string) (bool, error) {
	cache.checkedForBlob = true
	return false, nil
}

func (cache *testObjectStorageCache) uploadBlob(blobDigest string, body io.Reader) error {
	cache.uploadedBlob = true
	return nil
}

func TestFallbackForNotCacheablePath(t *testing.T) {
	observedZapCore, _ := observer.New(zap.InfoLevel)
	logger := zap.New(observedZapCore)

	cache := &testObjectStorageCache{}

	backendTestServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Hello, client")
	}))
	defer backendTestServer.Close()

	backendUrl, err := url.Parse(fmt.Sprint("http://", backendTestServer.Listener.Addr().String()))
	assert.Nil(t, err)

	fallbackReverseProxy := httputil.NewSingleHostReverseProxy(backendUrl)
	blo := NewBlobloProxy(backendUrl, cache, fallbackReverseProxy, logger)

	ts := httptest.NewServer(blo)
	defer ts.Close()

	client := http.Client{
		Timeout: 1 * time.Second,
	}
	client.Get(fmt.Sprint(ts.URL, "/some/blob/lo"))

	assert.False(t, cache.checkedForBlob)
	assert.False(t, cache.uploadedBlob)
	assert.False(t, cache.redirectedToPresignedUrl)
}
