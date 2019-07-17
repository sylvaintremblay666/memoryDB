package kvdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test010_putAndGetBack_noTx(t *testing.T) {
	err := Put("Key1", "Value1")
	assert.Nil(t, err, "Put should not return an error")

	var val string
	val, err = Get("Key1")
	assert.Nil(t, err)
	assert.Equal(t, val, "Value1", "Problem reading back inserted key")
}

func Test020_delete_noTx(t *testing.T) {
	Put("Key1", "Value1")
	val, err := Get("Key1")
	assert.Nil(t, err)
	assert.Equal(t, val, "Value1", "Problem reading back inserted key")

	Delete("Key1")
	_, err = Get("Key1")
	assert.NotNil(t, err, "The entry wasn't properly deleted")
}

func Test030_cannotCreateTransactionWithAlreadyActiveID(t *testing.T) {
	err := CreateTransaction("test030_tx1")
	assert.Nil(t, err, "Error while trying to create a transaction")

	err = CreateTransaction("test030_tx1")
	assert.NotNil(t, err, "Creating a transaction with an already active ID should fail")
}

func Test040_commitTransactionForPutWorksAsExpected(t *testing.T) {
	_, err := Get("test040_key1")
	assert.NotNil(t, err, "Value for key \"test040_key1\" should NOT be found")

	_, err = Get("test040_key2")
	assert.NotNil(t, err, "Value for key \"test040_key2\" should NOT be found")

	err = CreateTransaction("test040_tx")
	assert.Nil(t, err, "Error while trying to create a transaction")

	err = Put("test040_key1", "val1", "test040_tx")
	assert.Nil(t, err, "Putting a value in an active transaction should work")
	err = Put("test040_key2", "val2", "test040_tx")
	assert.Nil(t, err, "Putting a value in an active transaction should work")

	err = CommitTransaction("test040_tx")
	assert.Nil(t, err, "CommitTransaction should succeed")

	var val string
	val, err = Get("test040_key1")
	assert.Nil(t, err, "Should be able to get back a value modified in a committed transaction")
	assert.Equal(t, "val1", val)

	val, err = Get("test040_key2")
	assert.Nil(t, err, "Should be able to get back a value modified in a committed transaction")
	assert.Equal(t, "val2", val)
}

func Test050_wrongNbOfArgsWhenCallingPutReturnsAnError(t *testing.T) {
	err := Put("a")
	assert.NotNil(t, err, "Calling Put without enough arguments should fail")
}

func Test060_callingPutWithAnInactiveTransactionShouldFail(t *testing.T) {
	err := Put("key", "val", "ThisTXDoesNotExist")
	assert.NotNil(t, err, "Trying to put a value with an inactive transaction ID should fail")
}

func Test070_gettingValueModifiedWithinTransactionShouldReturnModifiedValue(t *testing.T) {
	err := Put("test070_key1", "val1")
	assert.Nil(t, err, "Put should not return an error")

	err = CreateTransaction("test070_tx")
	assert.Nil(t, err, "Error while trying to create a transaction")

	err = Put("test070_key1", "modifiedVal", "test070_tx")
	assert.Nil(t, err, "Put in an active transaction should not return any error")

	var val string
	val, err = Get("test070_key1", "test070_tx")
	assert.Nil(t, err, "Getting an existing value within a transaction should not return any error")
	assert.Equal(t, "modifiedVal", val, "Getting a value modified within a transaction should return the modified value")
}

func Test080_wrongNbOfArgsWhenCallingGetReturnsAnError(t *testing.T) {
	_, err := Get()
	assert.NotNil(t, err, "Calling Get without enough arguments should fail")
}

func Test090_tryingToGetFromAnInactiveTransactionIDShouldFail(t *testing.T) {
	_, err := Get("a", "b")
	assert.NotNil(t, err, "Trying to Get from an inactive transaction ID should fail")
}

func Test100_tryingToGetUnexistingValueFromAnActiveTransactionShouldFail(t *testing.T) {
	err := CreateTransaction("test100_tx")
	assert.Nil(t, err, "Error while trying to create a transaction")

	_, err = Get("Unexisting", "test100_tx")
	assert.NotNil(t, err, "Trying to get an unexisting value from a valid transaction should fail")
}

func Test110_tryingToGetValueDeletedWithinATransactionShouldFail(t *testing.T) {
	err := Put("test110_key1", "val1")
	assert.Nil(t, err, "Put should not return an error")

	err = CreateTransaction("test110_tx")
	assert.Nil(t, err, "Error while trying to create a transaction")

	var val string
	val, err = Get("test110_key1", "test110_tx")
	assert.Nil(t, err, "Getting existing value should not return an error")
	assert.Equal(t, "val1", val)

	Delete("test110_key1", "test110_tx")
	_, err = Get("test110_key1", "test110_tx")
	assert.NotNil(t, err, "Trying to get a deleted value should return an error")
}

func Test120_commitOfTransactionModifyingStuffShouldWorkAsExpected(t *testing.T) {
	err := Put("test120_key_to_delete", "val1")
	assert.Nil(t, err, "Put should not return an error")

	err = CreateTransaction("test120_tx")
	assert.Nil(t, err, "Error while trying to create a transaction")

	Delete("test120_key_to_delete", "test120_tx")
	_, err = Get("test120_key1", "test120_tx")
	assert.NotNil(t, err, "Trying to get a deleted value should return an error")

	var val string
	val, err = Get("test120_key_to_delete")
	assert.Nil(t, err, "Getting a value deleted within a tx not yet committed should still work outside of the tx")
	assert.Equal(t, "val1", val)

	err = Put("test120_newKey", "newVal", "test120_tx")
	assert.Nil(t, err, "Put in active transaction should not return an error")

	_, err = Get("test120_newKey")
	assert.NotNil(t, err, "A value created in a transaction not yet committed should NOT be found outside of the transaction")

	err = CommitTransaction("test120_tx")
	assert.Nil(t, err, "Transaction commit should work")

	_, err = Get("test120_key_to_delete")
	assert.NotNil(t, err, "A key deleted within a transaction which is now committed should be deleted")

	val, err = Get("test120_newKey")
	assert.Nil(t, err, "A key created within a transaction which is now committed should be found")
	assert.Equal(t, "newVal", val)
}

func Test130_wrongNbOfArgsWhenCallingDeleteReturnsAnError(t *testing.T) {
	err := Delete()
	assert.NotNil(t, err, "Calling Delete without enough arguments should fail")
}

func Test140_deletingFromAnInactiveTransactionReturnsAnError(t *testing.T) {
	err := Delete("key", "Unexisting140")
	assert.NotNil(t, err, "Trying to delete from an inactive transaction ID should return an error")
}

func Test150_tryingToCommitAnInactiveTransactionReturnsAnError(t *testing.T) {
	err := CommitTransaction("AnUnexistingTxID")
	assert.NotNil(t, err, "Trying to commit an inactive transaction ID should return an error")
}

func Test160_rollbackTransactionWorksAsExpected(t *testing.T) {
	err := Put("test160_key1", "val1")
	assert.Nil(t, err, "Put should not return an error")

	err = CreateTransaction("test160_tx")
	assert.Nil(t, err, "Error while trying to create a transaction")

	Delete("test160_key1", "test160_tx")
	_, err = Get("test160_key1", "test160_tx")
	assert.NotNil(t, err, "Trying to get a deleted value should return an error")

	RollbackTransaction("test160_tx")

	var val string
	val, err = Get("test160_key1")
	assert.Nil(t, err, "Getting a value deleted within a tx which have been rollbacked should still work")
	assert.Equal(t, "val1", val)
}

func Test170_tryingToCommitTransactionForWhomAModifiedValueMutatedShouldFail(t *testing.T) {
	err := Put("test170_key1", "val1")
	assert.Nil(t, err, "Put should not return an error")

	err = CreateTransaction("test170_tx")
	assert.Nil(t, err, "Error while trying to create a transaction")

	err = Put("test170_key1", "valFromTX", "test170_tx")
	assert.Nil(t, err, "Put should not return an error")

	err = Put("test170_key1", "mutate")
	assert.Nil(t, err, "Put should not return an error")

	err = CommitTransaction("test170_tx")
	assert.NotNil(t, err, "Trying to commit a transaction for whom a modified value mutated should fail")
}
