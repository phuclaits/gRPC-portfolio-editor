package user

import (
	"context"
	"gRPC-portfolio-editor/internal/pkg/db"
    pb "gRPC-portfolio-editor/internal/user/pb"
)

type Server struct {
	pb.UnimplementedUserServiceServer
}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	u := User{Email: req.Email, Password: req.Password}
	if err := db.DB.Create(&u).Error; err != nil {
		return &pb.RegisterResponse{Success: false, Error: err.Error()}, nil
	}
	return &pb.RegisterResponse{Success: true}, nil
}

func (s *Server) GetUserByEmail(ctx context.Context, req *pb.GetUserByEmailRequest) (*pb.GetUserByEmailResponse, error) {
	var u User
	if err := db.DB.Where("email = ?", req.Email).First(&u).Error; err != nil {
		return &pb.GetUserByEmailResponse{Error: err.Error()}, nil
	}
	return &pb.GetUserByEmailResponse{Id: uint64(u.ID), Email: u.Email}, nil
}
