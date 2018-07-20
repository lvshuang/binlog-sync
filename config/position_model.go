package main

import (
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"log"
)

type Pos struct {
	Name string `gorm:"column:name"`
	Pos  uint32 `gorm:"column:position"`
}

func (Pos) TableName() string {
	return "position"
}

type PosModel struct {
}

func NewPosModel() *Pos {
	return &Pos{}
}

func (m *Pos) GetByServerId(sid int32) *Pos{
	var pos Pos
	if err := Db.Find(&pos, "server_id", sid).Error; err != nil {
		log.Fatalln(err)
	}
	return &pos
}


