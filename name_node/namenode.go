package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"

	dataNode "github.com/benyamoulain/biblio/data_node/proto"
	nameNode "github.com/benyamoulain/biblio/name_node/proto"
)

var (
	port         = flag.Int("port", 10001, "El puerto del servidor")
	hostname     = flag.String("hostname", "localhost", "El hostname del servidor")
	distributed  = flag.Bool("distributed", false, "¿Usará exclusion mutua distribuida?")
	datanodePort = flag.Int("datanode_port", 10000, "Puerto de los NameNodes")

	// IPs : Arreglo con las ip's de las maquinas asignadas
	IPs = [...]string{"10.10.28.161", "10.10.28.162", "10.10.28.163"}
	// Map donde se guardarán los estados de los DataNodes
	aliveMap = make(map[string]bool)
)

type nameNodeServer struct {
	nameNode.UnimplementedNameNodeServiceServer
}

func newServer() *nameNodeServer {
	s := &nameNodeServer{}
	return s
}

func (s *nameNodeServer) Proposal(ctx context.Context, req *nameNode.ProposalRequest) (*nameNode.ProposalResponse, error) {
	requestDatanodes := req.GetIpList()
	ipList := getProposal(requestDatanodes)

	return &nameNode.ProposalResponse{
		IpList: ipList,
	}, nil
}

// checkAlive(ip) verifica si el datanode de la ip dada esta disponible
func checkAlive(ip string) bool {
	log.Printf("Verificando si el nodo %s está disponible...", ip)

	var opts []grpc.DialOption

	//Crear la conexión
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", ip, *datanodePort), opts...)
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

// getAlives() retorna la cantidad de datanodes disponibles
func getProposal(proposalDatanodes []string) []string {
	var newProposalDatanodes []string

	updateAlives()

	for i := 0; i < len(proposalDatanodes); i++ {
		if aliveMap[proposalDatanodes[i]] == true {
			newProposalDatanodes = append(newProposalDatanodes, proposalDatanodes[i])
		}
	}

	return newProposalDatanodes
}

func main() {
	flag.Parse()

	for i := 0; i < len(IPs); i++ {
		aliveMap[IPs[i]] = true
	}

	fmt.Println("Iniciando NameNode...")
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *hostname, *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("Escuchando en %s:%d", *hostname, *port)

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	nameNode.RegisterNameNodeServiceServer(grpcServer, newServer())
	grpcServer.Serve(lis)
}
