package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"

	dataNode "github.com/benyamoulain/biblio/data_node/proto"
)

var (
	port = flag.Int("port", 10000, "The server port")
)

const (
	chunkPath = "/chunks"
)

type dataNodeServer struct {
	dataNode.UnimplementedDataNodeServiceServer
}

func (s *dataNodeServer) Upload(stream dataNode.DataNodeService_UploadServer) error {
	log.Println("-> Llamada a la función Upload")
	for i := uint64(0); ; i++ {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&dataNode.UploadResponse{})
		}
		if err != nil {
			log.Fatalf("Ocurrió un error al llamar a la función Upload: %v", err)
			return err
		}
		// write to disk

		fileName := req.GetFileName()
		_, createErr := os.Create(fileName)

		if createErr != nil {
			fmt.Println(createErr)
			os.Exit(1)
		}
		partBuffer := req.GetChunkData()
		ioutil.WriteFile(fileName, partBuffer, os.ModeAppend)
	}
}

func newServer() *dataNodeServer {
	s := &dataNodeServer{}
	return s
}

func main() {
	fmt.Println("Starting server...")
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption

	grpcServer := grpc.NewServer(opts...)
	dataNode.RegisterDataNodeServiceServer(grpcServer, newServer())
	grpcServer.Serve(lis)
}
