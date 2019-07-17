package kvdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test001_readBackInsertedEntry_noTx(t *testing.T) {
	Put("Key1", "Value1")
	val, err := Get("Key1")
	assert.Nil(t, err)
	assert.Equal(t, val, "Value1", "Problem reading back inserted key")
}

func Test002_deleteExistingEntry_noTx(t *testing.T) {
	Put("Key1", "Value1")
	val, err := Get("Key1")
	assert.Nil(t, err)
	assert.Equal(t, val, "Value1", "Problem reading back inserted key")

	Delete("Key1")
	_, err = Get("Key1")
	assert.NotNil(t, err)
}
