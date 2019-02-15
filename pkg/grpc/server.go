package grpc

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"

	"github.com/Sirupsen/logrus"
	"github.com/kuberlab/pluk/pkg/api"
	plukio "github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/utils"
	"google.golang.org/grpc"
)

// server is used to implement PlukeServer.
type Server struct{}

// GetChunk implements PlukeServer
func (s *Server) GetChunk(ctx context.Context, in *ChunkRequest) (*ChunkResponse, error) {
	if ok, err := s.checkAuth(in.Auth); !ok {
		logrus.Error(err)
		return nil, err
	}
	reader, err := plukio.GetChunk(in.Path, byte(in.Version))
	if err != nil {
		logrus.Error(err)
		return nil, err
	}
	bt := bytes.NewBuffer([]byte{})
	io.Copy(bt, reader)
	return &ChunkResponse{Data: bt.Bytes()}, nil
}

func (s *Server) checkAuth(auth *Auth) (bool, error) {
	return api.GlobalAPI.CheckAuth(
		http.MethodGet,
		"",
		"",
		"",
		"",
		auth.Workspace,
		auth.Secret,
		nil,
	)
}

func Start() {
	logrus.Infof("Starting grpc server at :%v", utils.GrpcPort())
	lis, err := net.Listen("tcp", ":"+utils.GrpcPort())
	if err != nil {
		logrus.Errorf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	RegisterPlukeServer(s, &Server{})
	if err := s.Serve(lis); err != nil {
		logrus.Fatalf("failed to serve: %v", err)
	}
}
