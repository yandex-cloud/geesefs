package cfg

type ContentCache interface {
	GetContent(hash string, offset int64, length int64, opts struct{ RoutingKey string }) ([]byte, error)
	StoreContent(chunks chan []byte, hash string, opts struct{ RoutingKey string }) (string, error)
	StoreContentFromS3(source struct {
		Path        string
		BucketName  string
		Region      string
		EndpointURL string
		AccessKey   string
		SecretKey   string
	}, opts struct {
		RoutingKey string
		Lock       bool
	}) (string, error)
}
