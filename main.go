package main

import (
	"os"
	"time"

	"gopkg.in/mgo.v2"

	"github.com/labstack/gommon/log"

	"gitlab.com/k-terashima/go-bitflyer/v1/jsonrpc"
)

const (
	DBDIAL   = "localhost:27017"
	DBPREFIX = "test_"

	LOGOUTPUTLEVEL = log.INFO
)

func init() {
	// setup Logger
	f, err := os.OpenFile("server.log", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	log.SetLevel(LOGOUTPUTLEVEL)
	log.SetOutput(f)
}

func main() {
	done := make(chan struct{})

	sec, err := mgo.Dial(DBDIAL)
	if err != nil {
		log.Fatal(err)
	}
	defer sec.Close()

	btc := sec.DB(DBPREFIX + "bf").C("btc")
	if err != nil {
		log.Fatal(err)
	}
	fx := sec.DB(DBPREFIX + "bf").C("fxbtc")
	if err != nil {
		log.Fatal(err)
	}

	// 購読チャンネルを配列で設定
	// データベースを同数用意し各reading sectionに渡す
	channels := []string{"lightning_executions_BTC_JPY", "lightning_executions_FX_BTC_JPY"}
	dbs := []*mgo.Collection{btc, fx}

	for i, v := range channels {
		go reading(dbs[i], v)
	}

	<-done
}

// reading section
func reading(db *mgo.Collection, channel string) {
Reconnect:
	ch := make(chan jsonrpc.Response)
	go jsonrpc.Get([]string{
		channel,
	}, ch)
	time.Sleep(time.Second)

	defer close(ch)

	for {
		select {
		case v := <-ch:
			switch v.Type {
			case jsonrpc.Executions:
				go func() {
					for _, trade := range v.Executions {
						if err := db.Insert(&trade); err != nil {
							// log.Error(err)
						}

						// log.Debugf("%s: %+v\n", channel, trade)
					}
				}()

			case jsonrpc.Error:
				log.Error("websocket reconnect error, ", v.Error.Error())
				goto wsError

			}
		}
	}

wsError:
	close(ch)
	time.Sleep(3 * time.Second)
	if time.Now().Hour() == 4 { // メンテナンス時間に落ちた場合
		log.Infof("now time %s, therefore waiting websocket reconnect 12minutes", time.Now().Format("15:04"))
		time.Sleep(15 * time.Minute)
		log.Info("end bitflyer mentenance time, reconnect websocket")
	}
	goto Reconnect
}
