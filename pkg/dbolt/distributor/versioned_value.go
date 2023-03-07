package distributor

import (
	"encoding/json"
	"time"

	"github.com/pkg/errors"
)

func unmarshalVersionedValue(value []byte) (*VersionedValue, error) {
	versionedValue := new(VersionedValue)
	if err := json.Unmarshal(value, versionedValue); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal the value")
	}
	return versionedValue, nil
}

func marshalVersionedValue(versionedValue *VersionedValue) ([]byte, error) {
	marshaled, err := json.Marshal(versionedValue)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal the value")
	}
	return marshaled, nil
}

type VersionedValue struct {
	CreatedAt time.Time
	UpdatedAt time.Time
	Value     []byte
}

func newVersionedValueNow(value []byte) *VersionedValue {
	now := time.Now()
	return &VersionedValue{
		CreatedAt: now,
		UpdatedAt: now,
		Value:     value,
	}
}

func doNothing() {}
