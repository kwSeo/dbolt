package httpstore

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/go-kit/log"
)

const contentType = "application/json"

type HTTPKVClient struct {
	logger log.Logger
}

func New(logger log.Logger) *HTTPKVClient {
	return &HTTPKVClient{
		logger: logger,
	}
}

func (h *HTTPKVClient) Get(ctx context.Context, addr string, bucketName, key []byte) ([]byte, error) {
	reqBody := &GetReq{
		BucketName: bucketName,
		Key:        key,
	}
	marshaled, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}
	content := bytes.NewBuffer(marshaled)
	resp, err := http.Post(addr+"/v1/internal/get", contentType, content)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

func (h *HTTPKVClient) Put(ctx context.Context, addr string, bucketName, key, value []byte) error {
	reqBody := &PutReq{
		BucketName: bucketName,
		Key:        key,
		Value:      value,
	}
	marshaled, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}
	content := bytes.NewBuffer(marshaled)
	resp, err := http.Post(addr+"/v1/internal/put", contentType, content)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}
