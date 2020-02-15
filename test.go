package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
)
var wg sync.WaitGroup


type A struct {
	B string
}

func main() {
	var res bytes.Buffer
	encoder := gob.NewEncoder(&res)
	a := &A{"haha"}
	encoder.Encode(a)

	decoder := gob.NewDecoder(&res)
	_a := new(A)
	if err := decoder.Decode(_a); err != nil {
		fmt.Println(err)
	}
	fmt.Println(_a)
}


