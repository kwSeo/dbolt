package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/kwSeo/dbolt/example/grpc/helloworld"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"net"
	"time"
)

const (
	defaultName = "world"
)

var (
	appType    = flag.String("type", "server", "application type : server | client")
	serverPort = flag.Int("port", 50051, "The sever port")
	clientAddr = flag.String("addr", "localhost:50051", "the address to connect to")
	clientName = flag.String("name", defaultName, "Name to greet")
)

type server struct {
	helloworld.UnimplementedGreeterServer

	logger *zap.Logger
}

func (s *server) SayHello(ctx context.Context, in *helloworld.HelloRequest) (*helloworld.HelloReply, error) {
	s.logger.Info("Received.", zap.String("name", in.GetName()))
	return &helloworld.HelloReply{
		Message: "Hello " + in.GetName(),
	}, nil
}

func main() {
	flag.Parse()
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalln(err)
	}

	if *appType == "server" {
		logger.Info("Starting server...")
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *serverPort))
		if err != nil {
			logger.Fatal("Failed to listen.", zap.Error(err))
		}
		s := grpc.NewServer()
		helloworld.RegisterGreeterServer(s, &server{logger: logger})
		logger.Info("Server listening.", zap.String("addr", lis.Addr().String()))
		if err := s.Serve(lis); err != nil {
			logger.Fatal("Failed to serve.", zap.Error(err))
		}

	} else if *appType == "client" {
		logger.Info("Starting client...")
		conn, err := grpc.Dial(*clientAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			logger.Fatal("Did not connect.", zap.Error(err))
		}
		defer conn.Close()
		c := helloworld.NewGreeterClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		r, err := c.SayHello(ctx, &helloworld.HelloRequest{Name: *clientName})
		if err != nil {
			logger.Fatal("Could not greet.", zap.Error(err))
		}
		logger.Info("Greeting.", zap.String("message", r.GetMessage()))

	} else {
		logger.Fatal("Unsupported type.", zap.String("appType", *appType))
	}

}
