package main

import (
	"io"
	"net/http"
	"net/url"
	"strings"

	"go.uber.org/zap"
)

type BlobloProxy struct {
	upstreamUrl          *url.URL
	cache                ObjectStorageCache
	fallbackReverseProxy http.Handler
	logger               *zap.Logger
	isCacheableUri       func(requestURI string) bool
}

func NewBlobloProxy(upstreamUrl *url.URL, cache ObjectStorageCache, fallbackReverseProxy http.Handler, logger *zap.Logger) *BlobloProxy {
	return &BlobloProxy{
		upstreamUrl:          upstreamUrl,
		cache:                cache,
		fallbackReverseProxy: fallbackReverseProxy,
		logger:               logger,

		isCacheableUri: func(requestURI string) bool {
			pathElements := strings.Split(requestURI, "/")
			return len(pathElements) > 2 && pathElements[len(pathElements)-2] == "blobs"
		},
	}
}

func (blo *BlobloProxy) getUpstreamRequest(req *http.Request) *http.Request {
	upstreamReq := req.Clone(req.Context())
	upstreamReq.RequestURI = ""
	upstreamReq.Host = blo.upstreamUrl.Host
	upstreamReq.URL.Host = blo.upstreamUrl.Host
	upstreamReq.URL.Scheme = blo.upstreamUrl.Scheme
	return upstreamReq
}

func (blo *BlobloProxy) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	blo.logger.Info("Incoming request", zap.String("request", req.RequestURI), zap.String("method", req.Method))

	if req.Method == http.MethodGet && blo.isCacheableUri(req.RequestURI) {
		pathElements := strings.Split(req.RequestURI, "/")
		blobDigest := pathElements[len(pathElements)-1]

		headReq := blo.getUpstreamRequest(req)
		headReq.Method = http.MethodHead
		response, err := http.DefaultClient.Do(headReq)
		if err != nil {
			blo.logger.Error("Failed to reach the upstream", zap.String("error", err.Error()))
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer response.Body.Close()
		if response.StatusCode == http.StatusOK {
			isInCache, err := blo.cache.isBlobInCache(blobDigest)
			if err != nil {
				blo.logger.Error("Failed to check if object is in cache", zap.String("error", err.Error()))
			} else if isInCache {
				user, _, _ := headReq.BasicAuth()
				blo.logger.Info("Serving blob from cache", zap.String("digest", blobDigest), zap.String("user", user), zap.String("action", "serve_blob"))
				presignedUrl, err := blo.cache.getPresignedUrl(blobDigest)
				if err != nil {
					blo.logger.Error("Failed to get a presign url", zap.String("digest", blobDigest), zap.String("error", err.Error()))
					blo.fallbackReverseProxy.ServeHTTP(w, req)
					return
				}

				http.Redirect(w, req, presignedUrl, http.StatusFound)
				return
			} else { // upload the blob to cache and return the layer to the client
				upstreamReq := blo.getUpstreamRequest(req)
				response, err := http.DefaultClient.Do(upstreamReq)
				if err != nil {
					blo.logger.Error("Failed to reach the upstream", zap.String("error", err.Error()))
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				defer response.Body.Close()
				teeReader := io.TeeReader(response.Body, w)

				blo.logger.Info("Uploading blob to cache", zap.String("digest", blobDigest), zap.String("action", "upload_blob"))
				err = blo.cache.uploadBlob(blobDigest, teeReader)
				if err != nil {
					blo.logger.Error("Error uploading blob", zap.String("digest", blobDigest), zap.String("error", err.Error()))
				}

				return
			}
		}
	}

	blo.fallbackReverseProxy.ServeHTTP(w, req)
}
