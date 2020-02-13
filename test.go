package main

import (
	"fmt"
	"runtime/debug"
)

func main() {
	var arr = [5]int{1,2,3,4,5}
	changeArr(&arr)
	fmt.Println(arr)
}

func changeArr(arr *[5]int) {
	arr[0] = 45
	debug.PrintStack()
}
