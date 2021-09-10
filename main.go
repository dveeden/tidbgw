package main

import (
	"context"
	"flag"
	"io"
	"log"
	"math/rand"
	"net"
	"strings"
	"time"

	"go.etcd.io/etcd/clientv3"
)

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
			// TODO: Maybe use the HTTP status API to check the health of backends
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

	// TODO: Check for hosts that are gone
	rch := cli.Watch(context.Background(), "/topology/tidb/", clientv3.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {
			if strings.HasSuffix(string(ev.Kv.Key), "/info") {
				parts := strings.Split(string(ev.Kv.Key), "/")
				backendHostname := parts[3]

				for _, b := range backends {
					if b == backendHostname {
						goto end
					}
				}
				log.Printf("Adding backend %s\n", backendHostname)
				backends = append(backends, backendHostname)
			end:
			}
		}
	}
}
