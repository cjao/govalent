package main

import "crypto/md5"
import "encoding/hex"
import "fmt"
import "io"


func main() {
	Key := ""
	hasher := md5.New()
	b := make([]byte, 0)
	io.WriteString(hasher, Key)
	fmt.Printf("%s\n", hex.EncodeToString(hasher.Sum(b)))
}
