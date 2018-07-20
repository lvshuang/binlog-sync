package main

import (
	"binlog-sync/util"
	"binlog-sync/sync"
	"os"
	"os/signal"
	"syscall"
	"gopkg.in/birkirb/loggers.v1/log"
)

var signChannel = make(chan os.Signal, 1)

func main() {
	installSign()
	tables := []string{"table_name"}
	worker := sync.NewSync("127.0.0.1:3306", "root", "123456", "test", tables, 101)

	var waitWrapper = util.WaitGroupWrapper{}
	waitWrapper.Wrap(func() {worker.Run()})

	for {
		select {
			case <- signChannel:
				worker.ExitCh <- 1
				goto EXIT
		}
	}
	EXIT:
	waitWrapper.Wait()
	log.Infoln("main exit")
}

func installSign() {
	signal.Notify(signChannel, os.Interrupt, os.Kill, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
}
