package main

import (
	"fmt"
	"log"
	"time"

	"github.com/Part001-1/redis/internal/db"
)

func main() {

	// Init
	rdb, fCloseCon, err := db.InstanceRedis("localhost:6379", "", 0)
	if err != nil {
		log.Fatalf("fault create instance db:{%v}", err)
	}
	defer fCloseCon()

	iRedis, err := db.InterfaceRedis(rdb)
	if err != nil {
		log.Fatalf("fault create interface db:{%v}", err)
	}

	err = iRedis.PingRedis()
	if err != nil {
		log.Fatalf("fault PING Redis:{%v}", err)
	}

	// SET
	k := "testKey"
	v := "AAA-BBB-CCC"

	err = iRedis.SetData(k, v, time.Duration(1*time.Second))
	if err != nil {
		fmt.Printf("fault Set data:{%v}", err)
	}

	// GET
	rxValue, err := iRedis.GetData(k)
	if err != nil {
		fmt.Printf("fault Get data:{%v}\n", err)
	} else {
		fmt.Printf("Rx - > %s\n", rxValue)
	}

	time.Sleep(1 * time.Second)

	rxValue, err = iRedis.GetData(k)
	if err != nil {
		fmt.Printf("fault Get data:{%v}\n", err)
	} else {
		fmt.Printf("Rx - > %s\n", rxValue)
	}
}
