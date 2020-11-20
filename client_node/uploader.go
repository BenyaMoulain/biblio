package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"time"

	dataNode "github.com/benyamoulain/biblio/data_node/proto"
	"google.golang.org/grpc"
)

var (
	serverAddr = flag.String("server_addr", "localhost:10000", "The server address in the format of host:port")
)

func uploadFile(client dataNode.DataNodeServiceClient, fileName string) {
	fileToBeChunked := fileName // change here!

	file, err := os.Open(fileToBeChunked)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer file.Close()

	fileInfo, _ := file.Stat()

	var fileSize int64 = fileInfo.Size()

	const fileChunk = 250000 // 250 KB
	const chunkPath = "chunks/"

	// calculate total number of parts the file will be chunked into

	totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))

	fmt.Printf("Splitting to %d pieces.\n", totalPartsNum)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := client.Upload(ctx)
	if err != nil {
		log.Fatalf("%v.Upload(_) = _, %v", client, err)
	}

	for i := uint64(0); i < totalPartsNum; i++ {

		partSize := int(math.Min(fileChunk, float64(fileSize-int64(i*fileChunk))))
		partBuffer := make([]byte, partSize)

		_, err := file.Read(partBuffer)
		if err != nil {
			log.Fatalf("Error al leer el buffer: %v", err)
		}
		fileName := chunkPath + "book_" + strconv.FormatUint(i, 10)

		req := &dataNode.UploadRequest{
			FileName:  fileName,
			ChunkData: partBuffer,
		}

		// write/save buffer to disk
		sendErr := stream.Send(req)
		if sendErr != nil {
			log.Fatalf("Error al enviar el stream: %v", sendErr)
		}
		fmt.Println("Split to : ", fileName)
	}

	_, streamErr := stream.CloseAndRecv()
	if streamErr != nil {
		log.Fatalf("%v.CloseAndRecv() got error %v, want %v", stream, err, nil)
	}
}

func main() {
	os.Chdir("C:/Users/benja/Documents/MEGAsync/Distribuidos/Labs/lab2/biblio")

	flag.Parse()
	var opts []grpc.DialOption

	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(*serverAddr, opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	client := dataNode.NewDataNodeServiceClient(conn)
	uploadFile(client, "./book1.pdf")
}
