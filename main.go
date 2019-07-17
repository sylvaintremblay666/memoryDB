package main

import (
	"fmt"
	"memoryDB/kvdb"
	//  "net/http"
	//  "github.com/labstack/echo"
)

func main() {
	fmt.Println("hello world")

	kvdb.Put("Yo", "Ya")

	val, err := kvdb.Get("Yo")
	if err == nil {
		fmt.Println("Value: " + val)
	} else {
		fmt.Println("No value found, error is: " + err.Error())
	}

	val, err = kvdb.Get("Ya")
	if err == nil {
		fmt.Println("Value: " + val)
	} else {
		fmt.Println("Error: " + err.Error())
	}

	kvdb.Delete("Yo")

	val, err = kvdb.Get("Yo")
	if err == nil {
		fmt.Println("Value: " + val)
	} else {
		fmt.Println("No value found, error is: " + err.Error())
	}

  err = kvdb.CreateTransaction("toto")
  if err != nil {
    fmt.Println("Error creating transaction toto : " + err.Error())
  } else {
    fmt.Println("Transaction toto created")
  }

  err = kvdb.CreateTransaction("toto")
  if err != nil {
    fmt.Println("Error creating transaction toto : " + err.Error())
  } else {
    fmt.Println("Transaction toto created")
  }

  err = kvdb.Put("key", "val", "tx1")
  if err != nil {
    fmt.Println("Put error : " + err.Error())
  } else {
    fmt.Println("put ok")
  }

  err = kvdb.Put("key", "firstval")
  if err != nil {
    fmt.Println("Put error : " + err.Error())
  } else {
    fmt.Println("put ok")
  }


  err = kvdb.Put("key", "val", "toto")
  if err != nil {
    fmt.Println("TX Put error : " + err.Error())
  } else {
    fmt.Println("TX put ok")
  }

  err = kvdb.Put("key", "newval", "toto")
  if err != nil {
    fmt.Println("TX Put error : " + err.Error())
  } else {
    fmt.Println("TX put ok")
  }

  err = kvdb.Put("ted", "valted", "toto")
  if err != nil {
    fmt.Println("ted TX Put error : " + err.Error())
  } else {
    fmt.Println("ted TX put ok")
  }

  val, err = kvdb.Get("key")
  if err != nil {
    fmt.Println("Get error : " + err.Error())
  } else {
    fmt.Println("Get ok : " + val)
  }

  val, err = kvdb.Get("key", "toto")
  if err != nil {
    fmt.Println("TX Get error : " + err.Error())
  } else {
    fmt.Println("TX Get ok : " + val)
  }

  err = kvdb.Delete("key", "toto")
  if err != nil {
    fmt.Println("TX Delete error : " + err.Error())
  } else {
    fmt.Println("TX Delete ok")
  }

  val, err = kvdb.Get("key", "toto")
  if err != nil {
    fmt.Println("TX Get error : " + err.Error())
  } else {
    fmt.Println("TX Get ok : " + val)
  }

  val, err = kvdb.Get("key")
  if err != nil {
    fmt.Println("Get error : " + err.Error())
  } else {
    fmt.Println("Get ok : " + val)
  }
/*
  err = kvdb.Put("key", "mutated")
  if err != nil {
    fmt.Println("Put error : " + err.Error())
  } else {
    fmt.Println("put ok")
  }*/

  err = kvdb.Put("anotherKey", "anotherValue", "toto")
  if err != nil {
    fmt.Println("Put error : " + err.Error())
  } else {
    fmt.Println("put ok")
  }

  err = kvdb.CommitTransaction("toto")
  if err != nil {
    fmt.Println("Commit error : " + err.Error())
  } else {
    fmt.Println("Commit success")
  }

  val, err = kvdb.Get("key")
  if err != nil {
    fmt.Println("Get error : " + err.Error())
  } else {
    fmt.Println("Get ok : " + val)
  }

  val, err = kvdb.Get("anotherKey")
  if err != nil {
    fmt.Println("Get error : " + err.Error())
  } else {
    fmt.Println("Get ok : " + val)
  }

	/*
	   e := echo.New()
	   e.GET("/", func(c echo.Context) error {
	     return c.String(http.StatusOK, "Hello, World!")
	   })
	   e.Logger.Fatal(e.Start(":1323"))
	*/
}
