package main

import (
	"binlog-sync/util"
	"binlog-sync/sync"
	"os"
	"os/signal"
	"syscall"
	"gopkg.in/birkirb/loggers.v1/log"
	"binlog-sync/config"
)

var signChannel = make(chan os.Signal, 1)

func main() {
	installSign()

	nodes, err := config.GetNodes()
	if err != nil {
		log.Fatalf("Read config error: %v\n", err)
	}
	log.Printf("%v", nodes.Nodes)
	var waitWrapper = util.WaitGroupWrapper{}
	var workers []*sync.Sync

	for _, node := range nodes.Nodes {
		worker := sync.NewSync(node.Host, node.User, node.Password, node.Db, node.Tables, node.ServerId, node.Urls)
		waitWrapper.Wrap(func() {worker.Run()})
		workers = append(workers, worker)
	}

	for {
		select {
			case <- signChannel:
				CloseWorkers(workers)
				goto EXIT
		}
	}
	EXIT:
	waitWrapper.Wait()
	log.Infoln("main exit")
}

func CloseWorkers(workers []*sync.Sync) {
	for _, worker := range workers {
		close(worker.ExitCh)
	}
}

func installSign() {
	signal.Notify(signChannel, os.Interrupt, os.Kill, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
}
