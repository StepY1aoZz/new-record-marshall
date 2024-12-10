package main

import (
	"context"
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"os"
	"path"
	pb "record-write-testing/lib/proto"
	"strconv"
	"strings"
	"sync"
	"time"
)

const DBName = "db0"

var InputPath = flag.String("p", "./data/output", "Input files path")

var NumWorkers = flag.Int("num-workers", 2, "Number of workers")

var rowCnt = 0

func main() {
	conn, err := grpc.NewClient("127.0.0.1:8305", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	client := pb.NewWriteServiceClient(conn)
	dir, err := os.Open(*InputPath)
	if err != nil {
		panic(err)
	}
	names, err := dir.Readdirnames(-1)
	dataPool := sync.Pool{}
	for _, name := range names {
		dataPool.Put(name)
	}
	wg := &sync.WaitGroup{}
	wg.Add(*NumWorkers)
	start := time.Now()
	for i := 0; i < *NumWorkers; i++ {
		go write(&dataPool, wg, client)
	}
	wg.Wait()
	end := time.Now().Sub(start)

	rowRate := float64(rowCnt) / end.Seconds()
	fmt.Printf("loaded %d rows in %0.3fsec with %d workers (mean rate %0.2f rows/sec)\n", rowCnt, end.Seconds(), *NumWorkers, rowRate)
}

func write(dataPool *sync.Pool, wg *sync.WaitGroup, client pb.WriteServiceClient) {
	defer wg.Done()
	for {
		name, ok := dataPool.Get().(string)
		if !ok {
			break
		}
		dirPath := path.Join(*InputPath, name)
		f, err := os.Open(dirPath)
		if err != nil {
			panic(err)
		}
		bytes, err := io.ReadAll(f)
		if err != nil {
			panic(err)
		}
		req := &pb.WriteRequest{
			Database: DBName,
			Records: []*pb.Record{
				{
					Measurement: name,
					Block:       bytes,
				},
			},
		}
		_, err = client.Write(context.Background(), req)
		if err != nil {
			fmt.Printf("write error: %v\n", err)
			continue
		}
		args := strings.Split(name, "_")
		currRowCnt, err := strconv.Atoi(args[len(args)-1])
		if err != nil {
			panic(err)
		}
		rowCnt += currRowCnt
		f.Close()
	}
}
