package dbolt

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTokenFromBytes_bytes(t *testing.T) {
	token := TokenFromBytes([]byte{1, 2, 3, 4, 5})
	assert.Equal(t, uint32(15), token)
}

func TestTokenFromBytes_string(t *testing.T) {
	token := TokenFromBytes([]byte("hello world"))
	assert.Equal(t, uint32(1116), token)
}
