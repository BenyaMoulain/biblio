package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"

	dataNode "github.com/benyamoulain/biblio/data_node/proto"
	nameNode "github.com/benyamoulain/biblio/name_node/proto"
)

var (
	port         = flag.Int("port", 10000, "El puerto del servidor")
	hostname     = flag.String("hostname", "localhost", "El hostname del servidor")
	distributed  = flag.Bool("distributed", false, "¿Usará exclusion mutua distribuida?")
	chunkPath    = flag.String("chunk_path", "chunks", "Lugar donde los chunks serán guardados")
	namenodeIP   = flag.String("namenode_ip", "localhost", "Dirección IP del NameNode")
	namenodePort = flag.Int("namenode_port", 10001, "El puerto del NameNode")

	// IPs : Arreglo con las ip's de las maquinas asignadas
	IPs = [...]string{"10.10.28.161", "10.10.28.162", "10.10.28.163"}
	// Map donde se guardarán los estados de los DataNodes
	aliveMap = make(map[string]bool)
)

type dataNodeServer struct {
	dataNode.UnimplementedDataNodeServiceServer
}

func (s *dataNodeServer) Upload(stream dataNode.DataNodeService_UploadServer) error {
	log.Println("-> Llamada a la función Upload")

	var availableDN []string
	var chunksArray [][]byte
	var fileName string

	for i := uint64(0); ; i++ {

		req, err := stream.Recv()
		if err == io.EOF {
			log.Printf("Guardando archivos...")

			var chunkIndex int = 0

			availableDN = getProposal(getAlives())
			n := len(availableDN) + 1
			division := len(chunksArray) / n
			remainder := len(chunksArray) % n

			log.Printf("DN's disponibles: %d, Chunks por DN: %d, Chunks restantes: %d", n, division, remainder)

			for i := 0; i < division+remainder; i++ {
				fullFileName := fmt.Sprintf("%s/%s_%d.data", *chunkPath, fileName, i)
				_, createErr := os.Create(fullFileName)
				if createErr != nil {
					fmt.Println(createErr)
					os.Exit(1)
				}

				log.Printf("Guardando chunk %s", fullFileName)
				ioutil.WriteFile(fullFileName, chunksArray[i], os.ModeAppend)
				chunkIndex = division
			}
			for _, ip := range availableDN {
				for i := 0; i < division; i++ {
					indexedFileName := fmt.Sprintf("%s/%s_%d.data", *chunkPath, fileName, chunkIndex+i)
					sendChunk(ip, indexedFileName, chunksArray[chunkIndex+i])
				}
				chunkIndex += division
			}

			return stream.SendAndClose(&dataNode.UploadResponse{})
		}
		if err != nil {
			log.Fatalf("Ocurrió un error al llamar a la función Upload: %v", err)
			return err
		}
		// write to disk

		fileName = req.GetFileName()

		partBuffer := req.GetChunkData()

		chunksArray = append(chunksArray, partBuffer)
	}

	// ioutil.WriteFile(fileName, partBuffer, os.ModeAppend)
}

func (s *dataNodeServer) Alive(ctx context.Context, req *dataNode.AliveRequest) (*dataNode.AliveResponse, error) {
	return &dataNode.AliveResponse{}, nil
}

func (s *dataNodeServer) Recieve(ctx context.Context, req *dataNode.RecieveRequest) (*dataNode.RecieveResponse, error) {
	fileName := req.GetFileName()
	chunkData := req.GetChunkData()

	_, createErr := os.Create(fileName)
	if createErr != nil {
		fmt.Println(createErr)
		os.Exit(1)
	}
	ioutil.WriteFile(fileName, chunkData, os.ModeAppend)
	return &dataNode.RecieveResponse{}, nil
}

func newServer() *dataNodeServer {
	s := &dataNodeServer{}
	return s
}

// checkAlive(ip) verifica si el datanode de la ip dada esta disponible
func checkAlive(ip string) bool {
	log.Printf("Verificando si el nodo %s está disponible...", ip)

	var opts []grpc.DialOption

	//Crear la conexión
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", ip, *port), opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	// Enviar solicitud al servidor
	client := dataNode.NewDataNodeServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	aliveReq := &dataNode.AliveRequest{}
	_, aliveErr := client.Alive(ctx, aliveReq)
	if aliveErr != nil {
		log.Printf("%v.Alive(%s) = _, %v: ", client, ip, err)
		log.Printf("El nodo %s no está disponible.", ip)
		return false
	}
	log.Printf("El nodo %s si está disponible.", ip)
	return true
}

// updateAlives() actualiza el map de los estados de datanodes
func updateAlives() {
	for i := 0; i < len(IPs); i++ {
		if IPs[i] != *hostname {
			aliveMap[IPs[i]] = checkAlive(IPs[i])
		}
	}
}

// getAlives() retorna arreglo con datanodes disponibles
func getAlives() []string {
	var aliveDatanodes []string
	updateAlives()

	for i := 0; i < len(IPs); i++ {
		if (IPs[i] != *hostname) && (aliveMap[IPs[i]] == true) {
			aliveDatanodes = append(aliveDatanodes, IPs[i])
		}
	}
	return aliveDatanodes
}

// Envia propuesta de distribución al namenode y recibe la propuesta final
func getProposal(aliveDatanodes []string) []string {
	log.Printf("Enviando propuesta al NameNode %s...", *namenodeIP)

	var opts []grpc.DialOption

	//Crear la conexión
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", *namenodeIP, *namenodePort), opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	// Enviar solicitud al servidor
	client := nameNode.NewNameNodeServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	proposalReq := &nameNode.ProposalRequest{
		IpList: aliveDatanodes,
	}
	proposalDatanodes, proposalErr := client.Proposal(ctx, proposalReq)
	if proposalErr != nil {
		log.Fatalf("%v.Proposal(%s) = _, %v: ", client, aliveDatanodes, err)
	}
	return proposalDatanodes.GetIpList()
}

func sendChunk(ip string, fileName string, chunkData []byte) {
	log.Printf("Enviando chunk al DataNode %s...", ip)

	var opts []grpc.DialOption

	//Crear la conexión
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", ip, *port), opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	// Enviar solicitud al servidor
	client := dataNode.NewDataNodeServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	recieveReq := &dataNode.RecieveRequest{
		FileName:  fileName,
		ChunkData: chunkData,
	}
	_, proposalErr := client.Recieve(ctx, recieveReq)
	if proposalErr != nil {
		log.Fatalf("%v.Proposal(%s) = _, %v: ", client, ip, err)
	}
}

func main() {
	flag.Parse()

	algorithmText := "Exclusión Mutua Centralizada"
	if *distributed == true {
		algorithmText = "Exclusión Mutua Distribuida"
	}

	for i := 0; i < len(IPs); i++ {
		aliveMap[IPs[i]] = true
	}

	_ = os.Mkdir(*chunkPath, 0700)

	fmt.Printf("Iniciando servidor con algoritmo %s...\n", algorithmText)

	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *hostname, *port))
	if err != nil {
		log.Fatalf("failed to listen: %v \n", err)
	}
	log.Printf("Escuchando en %s:%d \n", *hostname, *port)
	var opts []grpc.ServerOption

	grpcServer := grpc.NewServer(opts...)
	dataNode.RegisterDataNodeServiceServer(grpcServer, newServer())
	grpcServer.Serve(lis)

}
