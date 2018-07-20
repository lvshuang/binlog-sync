package main

import (
	"github.com/spf13/viper"
	"github.com/siddontang/go-mysql/canal"
	"fmt"
)

var SyncCfgs []*canal.Config

func main() {
	viper.AddConfigPath("./config/conf.yml")
	viper.SetConfigType("yaml")

	nodes := viper.Get("nodes")
	fmt.Printf("%v\n", nodes)

}
