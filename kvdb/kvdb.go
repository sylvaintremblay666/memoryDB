package kvdb

import (
	"errors"
	"fmt"
)

type txData struct {
	origValuesMap map[string]string
	newValuesMap  map[string]string
}

const (
	deletedStr = "###DELETED###"
)

var (
	kvmap map[string]string
	txmap map[string]txData
)

func init() {
	fmt.Println("Initializing KvDB")
	kvmap = make(map[string]string)
	txmap = make(map[string]txData)
}

// Put can receive 2 or 3 parameters : k, v, transactionID. If no transactionID, operation is committed immediately
func Put(args ...string) error {
	if len(args) == 2 {
		kvmap[args[0]] = args[1]
		return nil
	}

	if len(args) == 3 {
		return putTX(args[0], args[1], args[2])
	}

	return errors.New("Wrong argument(s) using the Put function")
}

func putTX(k string, v string, transactionID string) error {
	var tx txData
	var ok bool
	if tx, ok = txmap[transactionID]; !ok {
		return errors.New("Transaction \"" + transactionID + "\" NOT found")
	}

	// If not saved yet in the origValuesMap, keep the original value
	if _, ok = tx.origValuesMap[k]; !ok {
		tx.origValuesMap[k] = kvmap[k]
	}

	// Then keep the change in the newValuesMap
	tx.newValuesMap[k] = v

	fmt.Println("After put:")
	fmt.Println(tx.origValuesMap) // to remove
	fmt.Println(tx.newValuesMap)  // to remove
	return nil
}

// Get can receive 1 or 2 parameters: k, transactionID.
func Get(args ...string) (string, error) {
	if len(args) == 1 {
		if val, ok := kvmap[args[0]]; ok {
			return val, nil
		}
		return "", errors.New("Key \"" + args[0] + "\" not defined")
	}

	if len(args) == 2 {
		return getTX(args[0], args[1])
	}

	return "", errors.New("Wrong argument(s) using the Put function")
}

func getTX(k string, transactionID string) (string, error) {
	var tx txData
	var ok bool
	var val string

	if tx, ok = txmap[transactionID]; !ok {
		return "", errors.New("Transaction \"" + transactionID + "\" NOT found")
	}

	if val, ok = tx.newValuesMap[k]; ok {
		if val != deletedStr {
			return val, nil
		}
		return "", errors.New("Key \"" + k + "\" not defined")
	}

	if val, ok := kvmap[k]; ok {
		return val, nil
	}

	return "", errors.New("Key \"" + k + "\" not defined")

}

// Delete can receive 1 or 2 parameters: k, transactionID.
func Delete(args ...string) error {
	if len(args) == 1 {
		delete(kvmap, args[0])
		return nil
	}

	if len(args) == 2 {
		return deleteTX(args[0], args[1])
	}

	return errors.New("Wrong argument(s) using the Put function")
}

func deleteTX(k string, transactionID string) error {
	var tx txData
	var ok bool
	if tx, ok = txmap[transactionID]; !ok {
		return errors.New("Transaction \"" + transactionID + "\" NOT found")
	}

	tx.newValuesMap[k] = deletedStr

	fmt.Println(tx) // to remove
	return nil
}

// CreateTransaction creates a transaction
func CreateTransaction(transactionID string) error {
	if _, ok := txmap[transactionID]; ok {
		return errors.New("Transaction \"" + transactionID + "\" is already active")
	}
	var data txData
	data.newValuesMap = make(map[string]string)
	data.origValuesMap = make(map[string]string)

	txmap[transactionID] = data
	return nil
}

// RollbackTransaction is used to rollback a transaction
func RollbackTransaction(transactionID string) {
	delete(txmap, transactionID)
}

// CommitTransaction is used to commit a transaction
func CommitTransaction(transactionID string) error {
	var tx txData
	var ok bool
	if tx, ok = txmap[transactionID]; !ok {
		return errors.New("Transaction \"" + transactionID + "\" NOT found")
	}

	// Make sure no key affected by the transaction mutated since it started. If so, rollback
	for k, v := range tx.origValuesMap {
		if v != kvmap[k] {
			delete(txmap, transactionID)
			return errors.New("The \"" + k + "\" key mutated since the transaction started, rolling back")
		}
	}

	// Apply the transaction modifications to the database
	for k, v := range tx.newValuesMap {
		if v == deletedStr {
			delete(kvmap, k)
		} else {
			kvmap[k] = v
		}
	}

	delete(txmap, transactionID)

	fmt.Println(tx)
	return nil
}
