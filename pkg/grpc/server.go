package grpc

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/kuberlab/pluk/pkg/api"
	plukio "github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
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
	bt := bytes.NewBuffer(make([]byte, 0, 16384))
	io.Copy(bt, reader)
	reader.Close()
	if bt.Len() == 0 {
		logrus.Warningf("Zero chunk response for %v", in.Path)
	}
	return &ChunkResponse{Data: bt.Bytes()}, nil
}

func (s *Server) checkAuth(auth *Auth) (bool, error) {
	return api.GlobalAPI.CheckAuth(
		http.MethodGet,
		"dataset",
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
	s := grpc.NewServer(
		grpc.WriteBufferSize(1024*32),
		grpc.ReadBufferSize(1024*32),
		grpc.MaxConcurrentStreams(64),
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: time.Duration(0)}),
	)
	RegisterPlukeServer(s, &Server{})
	if err := s.Serve(lis); err != nil {
		logrus.Fatalf("failed to serve: %v", err)
	}
}
