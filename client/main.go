package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"google.golang.org/grpc"
)

var (
	addr     = flag.String("addr", "127.0.0.1:20160", "address")
	logLevel = flag.String("loglevel", "info", "the level of log")
)

func cmdValidate(cmd string) error {
	return nil
}

func main() {
	flag.Parse()
	if *logLevel != "" {
		log.SetLevelByString(*logLevel)
	}

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.Infof("Client started")

	conn, err := grpc.Dial(*addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	log.Infof("Connected to Server")

	client := tinykvpb.NewTinyKvClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000)
	defer cancel()

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print(">>>")
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}

		words := strings.Split(strings.TrimSpace(line), " ")
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "get":
			if len(words) != 3 {
				fmt.Println("get has two args")
				continue
			}
			rsp, err := client.RawGet(ctx, &kvrpcpb.RawGetRequest{Cf: words[1], Key: []byte(words[2])})
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Printf("<<<%s\n", rsp.Value)
			}
		case "put":
			if len(words) != 4 {
				fmt.Println("put has three args")
				continue
			}
			rsp, err := client.RawPut(ctx, &kvrpcpb.RawPutRequest{Cf: words[1], Key: []byte(words[2]), Value: []byte(words[3])})
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Println(rsp.Error)
			}
		case "del":
			if len(words) != 3 {
				fmt.Println("del has two args")
				continue
			}
			rsp, err := client.RawDelete(ctx, &kvrpcpb.RawDeleteRequest{Cf: words[1], Key: []byte(words[2])})
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Println(rsp.Error)
			}
		case "scan":
			if len(words) != 4 {
				fmt.Println("put has three args")
				continue
			}
			limit, err := strconv.Atoi(words[3])
			if err != nil {
				fmt.Println(err)
				continue
			}
			rsp, err := client.RawScan(ctx, &kvrpcpb.RawScanRequest{Cf: words[1], StartKey: []byte(words[2]), Limit: uint32(limit)})
			if err != nil {
				fmt.Println(err)
			}

			if len(rsp.Kvs) > 0 {
				fmt.Printf("<<<")
			}

			for _, pair := range rsp.Kvs {
				fmt.Printf("%s:%s, ", string(pair.Key), string(pair.Value))
			}

			if len(rsp.Kvs) > 0 {
				fmt.Println("")
			}
		default:
			fmt.Println("invalid command")
		}
	}

	log.Info("Client stopped.")
}
