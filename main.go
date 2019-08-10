package main

import (
	"fmt"
	"time"

	"gopkg.in/mgo.v2"

	"github.com/labstack/gommon/log"

	"gitlab.com/k-terashima/go-bitflyer/v1/jsonrpc"
)

const (
	DBDIAL   = "localhost:27017"
	DBPREFIX = "test_"
)

func main() {
	done := make(chan struct{})

	sec, err := mgo.Dial(DBDIAL)
	if err != nil {
		log.Fatal(err)
	}
	defer sec.Close()

	btc := sec.DB(DBPREFIX + "bitflyer").C("btcjpy")
	if err != nil {
		log.Fatal(err)
	}
	fx := sec.DB(DBPREFIX + "bitflyer").C("btcjpy")
	if err != nil {
		log.Fatal(err)
	}

	go func() { // 現物約定
	Reconnect:
		ch := make(chan jsonrpc.Response)
		go jsonrpc.Get([]string{
			"lightning_executions_BTC_JPY",
		}, ch)
		time.Sleep(time.Second)

		for {
			select {
			case v := <-ch:
				switch v.Type {
				case jsonrpc.Executions:
					go func() {
						for _, trade := range v.Executions {
							if err := btc.Insert(&trade); err != nil {
								log.Error(err)
							}

							fmt.Printf("現物: %+v\n", trade)
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
	}()

	go func() { // FX約定
	Reconnect:
		ch := make(chan jsonrpc.Response)
		go jsonrpc.Get([]string{
			"lightning_executions_FX_BTC_JPY",
		}, ch)
		time.Sleep(time.Second)

		for {
			select {
			case v := <-ch:
				switch v.Type {
				case jsonrpc.Executions:
					go func() {
						for _, trade := range v.Executions {
							if err := fx.Insert(&trade); err != nil {
								log.Error(err)
							}
							fmt.Printf("FX: %+v\n", trade)
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
	}()

	<-done
}
