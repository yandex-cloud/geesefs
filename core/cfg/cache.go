package cfg

import blobcache "github.com/beam-cloud/blobcache-v2/pkg"

type ContentCache interface {
	GetContent(hash string, offset int64, length int64, opts struct{ RoutingKey string }) ([]byte, error)
	StoreContent(chunks chan []byte, hash string, opts struct{ RoutingKey string }) (string, error)
	StoreContentFromS3(source blobcache.ContentSourceS3, opts blobcache.StoreContentOptions) (string, error)
}
