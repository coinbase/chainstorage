package main

import (
	"log"
	"os"
)

func main() {
	if len(os.Args) != 3 {
		log.Fatalln("Invalid number of arguments. expected: `config_gen /path/to/chainstorage/configs /path/to/chainstorage`")
	}
	configGenerator := NewConfigGenerator(os.Args[1], os.Args[2])
	err := configGenerator.Run()
	if err != nil {
		log.Fatal(err)
	}
}
