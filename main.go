package main

import (
	"fmt"
	"memoryDB/kvdb"
)

func main() {
  // I'm NOT doing extended testing / validation / error checking in this main
  // but simply using the api expecting it to work properly. Look at the
  // "kvdb/kvdb_test.go" file for real tests with 100% coverage.

  // First scenario from the exercise (no tx)
  fmt.Println("---== First Scenario ==---")
	kvdb.Put("example", "foo")

  val, _ := kvdb.Get("example")
  fmt.Println("Value for example : " + val)

  kvdb.Delete("example")

  _, err := kvdb.Get("example")
  if err != nil {
    fmt.Println(err.Error())
  }

  kvdb.Delete("example")
  _, err = kvdb.Get("example")
  if err != nil {
    fmt.Println(err.Error())
  }

  fmt.Println()

  // Second scenario from the exercise
  fmt.Println("---== Second Scenario ==---")

  kvdb.CreateTransaction("abc")
  kvdb.Put("a", "foo", "abc")
  val, _ = kvdb.Get("a", "abc")
  fmt.Println("Value for key \"a\" in tx \"abc\": " + val)

  _, err = kvdb.Get("a")
  if err != nil {
    fmt.Println(err.Error())
  }

  kvdb.CreateTransaction("xyz")
  kvdb.Put("a", "bar", "xyz")
  val, _ = kvdb.Get("a", "xyz")
  fmt.Println("Value for key \"a\" in tx \"xyz\": " + val)

  kvdb.CommitTransaction("xyz")
  val, _ = kvdb.Get("a")
  fmt.Println("Value for key \"a\": " + val)

  err = kvdb.CommitTransaction("abc")
  if err != nil {
    fmt.Println(err.Error())
  }

  val, _ = kvdb.Get("a")
  fmt.Println("Value for key \"a\": " + val)

  kvdb.CreateTransaction("abc")
  kvdb.Put("a", "foo", "abc")
  val, _ = kvdb.Get("a")
  fmt.Println("Value for key \"a\": " + val)
  kvdb.RollbackTransaction("abc")
  err = kvdb.Put("a", "foo", "abc")
  if err != nil {
    fmt.Println(err.Error())
  }

  val, _ = kvdb.Get("a")
  fmt.Println("Value for key \"a\": " + val)

  kvdb.CreateTransaction("def")
  err = kvdb.Put("b", "foo", "def")
  if err != nil {
    fmt.Println(err.Error())
  } else {
    fmt.Println("According to the scenario this Put should fail, but I think its an error in the scenario...?")
  }

  val, _ = kvdb.Get("a", "def")
  fmt.Println("Value for key \"a\": " + val)

  kvdb.RollbackTransaction("def")

}
