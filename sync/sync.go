package sync

import (
	"github.com/siddontang/go-mysql/canal"
	"github.com/syndtr/goleveldb/leveldb"
	"gopkg.in/birkirb/loggers.v1/log"
	"github.com/siddontang/go-mysql/mysql"
	"binlog-sync/util"
	"encoding/json"
)

type Sync struct {
	Addr        string
	User        string
	Password    string
	Db          string
	Tables      []string
	ServerId    uint32
	Urls        []string
	ExitCh      chan int
	canal       *canal.Canal
	WaitWrapper util.WaitGroupWrapper
	Dispatch    *Dispatch
}

func NewSync(addr, user, password, db string, tables []string, serverId uint32, urls []string) *Sync {
	sync := new(Sync)
	sync.Addr = addr
	sync.User = user
	sync.Password = password
	sync.Db = db
	sync.Tables = tables
	sync.ServerId = serverId
	sync.Urls = urls
	sync.ExitCh = make(chan int)

	return sync
}

func (s *Sync) init() error  {
	return nil
}

func (s *Sync) Run() {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = s.Addr
	cfg.User = s.User
	cfg.Password = s.Password
	cfg.ServerID = s.ServerId
	cfg.Dump.TableDB = s.Db
	cfg.Dump.Tables = s.Tables

	c, err := canal.NewCanal(cfg)
	if err != nil {
		log.Fatalf("New Canel error: %v\n", err)
	}

	dispatcher := &Dispatch{
		ExitCh: make(chan int),
		EvCh: make(chan *canal.RowsEvent, 10),
	}
	s.Dispatch = dispatcher
	s.WaitWrapper.Wrap(func() {dispatcher.Loop(s.Urls)})

	handler := &MyEventHandler{ServerId: s.ServerId, Dispatcher: dispatcher}
	c.SetEventHandler(handler)
	s.canal = c
	s.WaitWrapper.Wrap(func () {s.monitorExit()})
	db, err := leveldb.OpenFile("./db.data", nil)
	if err != nil {
		log.Fatalf("%v\n", err)
	}
	has, _ := db.Has(mysql.Uint32ToBytes(s.ServerId), nil)
	if !has {
		db.Close()
		log.Infoln("load position from master")
		pos, err := c.GetMasterPos()
		if err != nil {
			log.Fatalf("Get master positon error: %v", err)
		}
		c.RunFrom(pos)
	} else {
		log.Infoln("load position from local db")
		posJson, err := db.Get(mysql.Uint32ToBytes(s.ServerId), nil)
		db.Close()
		if err != nil {
			log.Fatalf("Get position from local db failed: %v", err)
		}
		var pos mysql.Position
		err = json.Unmarshal([]byte(posJson), &pos)
		if err != nil {
			log.Fatalf("Parse position from local db failed: %v", err)
		}
		c.RunFrom(pos)
	}
	log.Infoln("wait ev handler stop")
	s.WaitWrapper.Wait()
}

func (s *Sync) monitorExit() error {
	for  {
		select {
			case <- s.ExitCh:
				s.canal.Close()
				close(s.Dispatch.ExitCh)
			    goto EXIT
		}
	}
	EXIT:
		return nil
}


func (s *Sync) Close() {
	s.canal.Close();
}