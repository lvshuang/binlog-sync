package config

import (
	"github.com/spf13/viper"
)

type Node struct {
	Host string
	ServerId uint32
	User string
	Password string
	Db string
	Tables []string
	Urls []string
}

type Nodes struct {
	Nodes []Node
}

func GetNodes() (Nodes, error) {
	viper.SetConfigFile("./config/conf.toml")
	viper.SetConfigType("toml")
	var nodes Nodes
	err := viper.ReadInConfig()
	if err != nil {
		return nodes, err
	}

	err = viper.Unmarshal(&nodes)
	if err != nil {
		return nodes, err
	}
	return nodes, nil
}
