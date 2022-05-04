package main

import (
	"fmt"
	"io"
	"io/ioutil"
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

	defaultPresignedUrl string
}

func (cache *testObjectStorageCache) getPresignedUrl(blobDigest string) (string, error) {
	cache.redirectedToPresignedUrl = true
	return cache.defaultPresignedUrl, nil
}

func (cache *testObjectStorageCache) isBlobInCache(blobDigest string) (bool, error) {
	cache.checkedForBlob = true

	return cache.uploadedBlob, nil
}

func (cache *testObjectStorageCache) uploadBlob(blobDigest string, body io.Reader) error {
	cache.uploadedBlob = true
	io.ReadAll(body)
	return nil
}

type testBackend struct {
	server *httptest.Server

	defaultResponse string
	url             *url.URL
}

func newTestBackend(defaultResponse string) (*testBackend, error) {
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, defaultResponse)
	}))
	backendUrl, err := url.Parse(fmt.Sprint("http://", testServer.Listener.Addr().String()))
	if err != nil {
		testServer.Close()
	}

	return &testBackend{
		server:          testServer,
		defaultResponse: defaultResponse,
		url:             backendUrl,
	}, err
}

type testFixture struct {
	cache         *testObjectStorageCache
	tBlobloServer *httptest.Server
	tBloblo       *BlobloProxy
	tBackend      *testBackend
}

func newTestFixture(t *testing.T) *testFixture {
	observedZapCore, _ := observer.New(zap.InfoLevel)
	logger := zap.New(observedZapCore)

	cache := &testObjectStorageCache{
		defaultPresignedUrl: "http://localtest.me/a_presigned_url",
	}

	backendTestServer, err := newTestBackend("test response")
	assert.Nil(t, err)

	fallbackReverseProxy := httputil.NewSingleHostReverseProxy(backendTestServer.url)
	blo := NewBlobloProxy(backendTestServer.url, cache, fallbackReverseProxy, logger)

	return &testFixture{
		cache:         cache,
		tBlobloServer: httptest.NewServer(blo),
		tBloblo:       blo,
		tBackend:      backendTestServer,
	}
}

func TestFallbackForNotCacheablePath(t *testing.T) {
	fixture := newTestFixture(t)
	defer fixture.tBlobloServer.Close()
	defer fixture.tBackend.server.Close()

	client := http.Client{
		Timeout: 1 * time.Second,
	}
	resp, err := client.Get(fmt.Sprint(fixture.tBlobloServer.URL, "/some/blob/lo"))
	assert.Nil(t, err)
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	assert.Equal(t, fixture.tBackend.defaultResponse, string(body))

	assert.False(t, fixture.cache.checkedForBlob)
	assert.False(t, fixture.cache.uploadedBlob)
	assert.False(t, fixture.cache.redirectedToPresignedUrl)
}

func TestBlobInitiallyNotInCache(t *testing.T) {
	cache := &testObjectStorageCache{}
	testBlobPath := "/v2/blobs/sha256:891b05d87f5e008949d4caf55929c31c3aab0ecbd5ae19e40e8f1421ffd612dd"
	blobInCache, err := cache.isBlobInCache(testBlobPath)

	assert.False(t, blobInCache)
	assert.Nil(t, err)
}

func TestBlobIsUploadedToCacheAndReturnedToClient(t *testing.T) {
	fixture := newTestFixture(t)
	defer fixture.tBlobloServer.Close()
	defer fixture.tBackend.server.Close()

	client := http.Client{
		Timeout: 1 * time.Second,
	}
	cacheablePath := "/v2/blobs/sha256:891b05d87f5e008949d4caf55929c31c3aab0ecbd5ae19e40e8f1421ffd612dd"
	resp, err := client.Get(fmt.Sprint(fixture.tBlobloServer.URL, cacheablePath))
	assert.Nil(t, err)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)

	assert.Equal(t, fixture.tBackend.defaultResponse, string(body))
	assert.True(t, fixture.cache.checkedForBlob)
	assert.True(t, fixture.cache.uploadedBlob)
	assert.False(t, fixture.cache.redirectedToPresignedUrl)
}

func TestClientIsRedirectedWhenBlobInCache(t *testing.T) {
	fixture := newTestFixture(t)
	defer fixture.tBlobloServer.Close()
	defer fixture.tBackend.server.Close()

	client := http.Client{
		Timeout: 1 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	cacheablePath := "/v2/blobs/sha256:891b05d87f5e008949d4caf55929c31c3aab0ecbd5ae19e40e8f1421ffd612dd"
	resp, err := client.Get(fmt.Sprint(fixture.tBlobloServer.URL, cacheablePath))
	assert.Nil(t, err)
	defer resp.Body.Close()
	resp, err = client.Get(fmt.Sprint(fixture.tBlobloServer.URL, cacheablePath))
	assert.Nil(t, err)
	defer resp.Body.Close()

	location, err := resp.Location()
	assert.Nil(t, err)

	assert.Equal(t, http.StatusFound, resp.StatusCode)
	assert.Equal(t, fixture.cache.defaultPresignedUrl, location.String())
	assert.True(t, fixture.cache.redirectedToPresignedUrl)
}

func TestCustomCacheableFilter(t *testing.T) {
	fixture := newTestFixture(t)
	defer fixture.tBlobloServer.Close()
	defer fixture.tBackend.server.Close()

	client := http.Client{
		Timeout: 1 * time.Second,
	}
	requestPath := "/v2/blobs/sha256:891b05d87f5e008949d4caf55929c31c3aab0ecbd5ae19e40e8f1421ffd612dd"

	fixture.tBloblo.isCacheableUri = func(requestURI string) bool {
		return false
	}
	resp, err := client.Get(fmt.Sprint(fixture.tBlobloServer.URL, requestPath))
	assert.Nil(t, err)
	defer resp.Body.Close()
	assert.False(t, fixture.cache.checkedForBlob)

	fixture.tBloblo.isCacheableUri = func(requestURI string) bool {
		return true
	}
	resp, err = client.Get(fmt.Sprint(fixture.tBlobloServer.URL, requestPath))
	assert.Nil(t, err)
	defer resp.Body.Close()
	assert.True(t, fixture.cache.checkedForBlob)
}
