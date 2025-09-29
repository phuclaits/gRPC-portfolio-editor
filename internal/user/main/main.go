package main

import (
	"log"
	"net"

	"gRPC-portfolio-editor/internal/pkg/db"
	"gRPC-portfolio-editor/internal/user"
	pb "gRPC-portfolio-editor/internal/user/pb"

	"google.golang.org/grpc"
)

func main() {
	dsn := "postgres://postgres:postgres@localhost:5432/portfolio_db?sslmode=disable"
	db.ConnectDB(dsn)
	db.DB.AutoMigrate(&user.User{})

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterUserServiceServer(s, user.NewServer())
	log.Println("User gRPC Service listening on :50051")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
