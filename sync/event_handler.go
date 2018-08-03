package sync

import (
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"gopkg.in/birkirb/loggers.v1/log"
	"github.com/syndtr/goleveldb/leveldb"
	"encoding/json"
	"binlog-sync/util"
	"net/http"
	"strings"
	"io/ioutil"
	"time"
	"fmt"
	"errors"
)

var retryIntervalTimes = []int64{1, 15, 45, 120, 300, 900, 1920, 3840, 10800, 18000}

type Dispatch struct {
	EvCh chan *canal.RowsEvent
	ExitCh chan int
	util.WaitGroupWrapper
	Urls []string
	FailedEvs []*FailedEv
}

type FailedEv struct {
	Ev *canal.RowsEvent
	PostJson string
	Url string
	RetryTimes int64    // 重试次数
	LastRetryTime int64 // 上一次重试时间
	PushTime int64
}

func (d *Dispatch) Loop(urls []string) {
	for _, url := range urls {
		d.Urls = append(d.Urls, url)
	}
	for i := 0; i < 5; i ++ {
		d.Wrap(func() {d.loop()})
	}
	d.Wrap(func() {d.loopFailedEv()})
	d.Wait()
	log.Infoln("dispatch exited")
}

func (d *Dispatch) loop() {
	for  {
		select {
		case ev := <- d.EvCh:
			d.dispatch(ev)
		case <- d.ExitCh:
			goto EXIT
		}
	}
	EXIT:
}

// 处理失败请求
func (d *Dispatch) loopFailedEv() {
	log.Infoln("start failed event processor")
	td := time.Duration(time.Second * 1)
	ticker := time.NewTicker(td)
	for {
		select {
			case <- ticker.C:
				d.dispatchFailedEv()
			case <- d.ExitCh:
				goto EXIT
		}
	}
	ticker.Stop()
	EXIT:
}

func (d *Dispatch) dispatchFailedEv() {
	t := time.Now()
	timestamp := t.Unix()
	for key, failedEv := range d.FailedEvs {
		var pushTime int64
		if failedEv.LastRetryTime > 0 {
			pushTime = failedEv.LastRetryTime + retryIntervalTimes[failedEv.RetryTimes - 1]
		} else {
			pushTime = failedEv.PushTime + retryIntervalTimes[failedEv.RetryTimes -1]
		}
		// 还没达到重试时间
		if (pushTime > timestamp) {
			continue
		}

		d.FailedEvs = append(d.FailedEvs[:key], d.FailedEvs[key+1:]...) // 从重试队列中删除当前元素
		if (failedEv.RetryTimes > 10) {
			log.Printf("ev: %v retry times > 10, stop retry", failedEv.Ev)
			continue
		}
		
		err := d.httpPost(failedEv.Url, failedEv.PostJson, failedEv.RetryTimes)
		if err != nil {
			fEv := &FailedEv{
				Ev: failedEv.Ev,
				PostJson: failedEv.PostJson,
				RetryTimes: failedEv.RetryTimes + 1,
				Url: failedEv.Url,
				PushTime: timestamp,
				LastRetryTime: timestamp,
			}
			d.FailedEvs = append(d.FailedEvs, fEv)
		}
	}
}

func (d *Dispatch) dispatch(ev *canal.RowsEvent) {
	jsonBytes, _ := json.Marshal(ev)
	postJson := string(jsonBytes)
	t := time.Now()
	timestamp := t.Unix()
	var retryTime int64
	retryTime = 0
	for _, url := range d.Urls {
		if len(url) < 1 {
			continue
		}
		err := d.httpPost(url, postJson, retryTime)
		if err != nil {
			fEv := &FailedEv{
				Ev: ev,
				PostJson: postJson,
				RetryTimes: retryTime + 1,
				Url: url,
				PushTime: timestamp,
			}
			d.FailedEvs = append(d.FailedEvs, fEv)
		}
	}
}

func (d *Dispatch) httpPost(url string, postJson string, retryTime int64) error {
	log.Printf("push to url: %s\n, data: %s, retry time: %d\n", url, postJson, retryTime)
	resp, err := http.Post(url, "application/json", strings.NewReader(postJson))
	if err != nil {
		log.Errorf("push to url failed, url: %s, data: %s, error: %v\n", url, postJson, err)
		return err
	}
	if resp.StatusCode != 200 {
		log.Errorf("push to url failed, url: %s, data: %s, error: http code is %d\n", url, postJson, resp.StatusCode)
		return errors.New(fmt.Sprintf("response http code is %d", resp.StatusCode))
	}
	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		log.Errorf("read response from %s failed, data: %s, error: %v\n", url, postJson, err)
		return err
	}
	log.Printf("push to %s success, data: %s, response: %s\n", url, postJson, string(body))
	return nil
}

type MyEventHandler struct {
	canal.DummyEventHandler
	ServerId uint32
	Dispatcher *Dispatch
}

func (h *MyEventHandler) OnRow(e *canal.RowsEvent) error {
	h.Dispatcher.EvCh <- e
	//fmt.Printf("事件: %s  值: %v\n", e.Action, e.Rows)
	return nil
}

func (h *MyEventHandler) String() string {
	return "MyEventHandler"
}

func (h *MyEventHandler) OnPosSynced(pos mysql.Position, force bool) error {
	if !force {
		return nil
	}
	db, err := leveldb.OpenFile("./db.data", nil)
	if err != nil {
		log.Errorf("save position failed, open db err: %v\n", err)
		return err
	}
	defer db.Close()
	posBytes, err := json.Marshal(pos)
	err = db.Put(mysql.Uint32ToBytes(h.ServerId), posBytes, nil)
	if err != nil {
		log.Errorf("save position failed, open db err: %v\n", err)
		return err
	}
	log.Infoln("Save position success, serverId: %d, position: %v", h.ServerId, pos)

	return nil
}
