package httpstore

type GetReq struct {
	BucketName []byte `json:"bucketName"`
	Key        []byte `json:"key"`
}

type PutReq struct {
	BucketName []byte `json:"bucketName"`
	Key        []byte `json:"key"`
	Value      []byte `json:"value"`
}
