package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type status struct {
	Version         string
	Git_hash        string
	Status_port     int
	Deploy_path     string
	Start_timestamp int32
	Labels          []string `json:"-"`
}

func connectionHandler(listener net.Listener, backends *[]string) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}

		if len(*backends) == 0 {
			log.Println("No backends available")
			conn.Close()
		} else {
			rand.Seed(time.Now().UnixNano())
			choice := rand.Int() % len(*backends)
			backend := (*backends)[choice]
			go proxyConn(conn, backend)
		}
	}
}

func proxyConn(conn net.Conn, backend string) {
	bConn, err := net.Dial("tcp", backend)
	if err != nil {
		log.Printf("Failed backend connection to %s", backend)
	} else {
		log.Printf("Connection to backend %s", backend)
		go io.Copy(bConn, conn)
		io.Copy(conn, bConn)
		conn.Close()
	}
}

func main() {
	pdAddr := flag.String("pdaddr", "127.0.0.1:2379", "PD Address")
	listenAddr := flag.String("listen", "127.0.0.1:4009", "Listen Address")
	flag.Parse()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{*pdAddr},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatal(err)
	}

	backends := []string{}
	go connectionHandler(listener, &backends)

	rch := cli.Watch(context.Background(), "/topology/tidb/",
		clientv3.WithPrefix(),
		clientv3.WithPrevKV(),
	)

	for wresp := range rch {
		for _, ev := range wresp.Events {
			if strings.HasSuffix(string(ev.Kv.Key), "/info") {
				parts := strings.Split(string(ev.Kv.Key), "/")
				backendHostname := parts[3]
				j := status{}
				err = json.Unmarshal(ev.Kv.Value, &j)
				if err != nil {
					log.Fatal(err)
				}
				resp, err := http.Get(
					fmt.Sprintf(
						"http://%s:%d/", strings.Split(backendHostname, ":")[0],
						j.Status_port,
					),
				)
				if err != nil {
					log.Fatal(err)
				}
				log.Printf("Status probe for %s has response: %s (not used)\n", backendHostname, resp.Status)
			}
			if strings.HasSuffix(string(ev.Kv.Key), "/ttl") {
				parts := strings.Split(string(ev.Kv.Key), "/")
				backendHostname := parts[3]

				switch ev.Type {
				case clientv3.EventTypePut:
					for _, b := range backends {
						if b == backendHostname {
							goto end
						}
					}
					log.Printf("Adding backend %s\n", backendHostname)
					backends = append(backends, backendHostname)
				case clientv3.EventTypeDelete:
					log.Printf("Removing backend %s\n", backendHostname)
					for i, b := range backends {
						if b == backendHostname {
							backends = append(backends[:i], backends[i+1:]...)
							goto end
						}
					}
				}
			end:
			}
		}
	}
}
