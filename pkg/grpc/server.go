package grpc

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/kuberlab/pluk/pkg/api"
	plukio "github.com/kuberlab/pluk/pkg/io"
	"github.com/kuberlab/pluk/pkg/utils"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// server is used to implement PlukeServer.
type Server struct{}

// GetChunk implements PlukeServer
func (s *Server) GetChunk(_ context.Context, in *ChunkRequest) (*ChunkResponse, error) {
	if ok, err := s.checkAuth(in.Auth); !ok {
		logrus.Error(err)
		return nil, err
	}

	getData := func(path string, version byte) ([]byte, error) {
		reader, err := plukio.GetChunk(path, version)
		if err != nil {
			logrus.Error(err)
			return nil, err
		}
		bt := bytes.NewBuffer([]byte{})
		io.Copy(bt, reader)
		_ = reader.Close()
		return bt.Bytes(), nil
	}

	data, err := getData(in.Path, byte(in.Version))
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		logrus.Warningf("Zero chunk response for %v, re-requesting", in.Path)
		os.Remove(in.Path)
		data, err = getData(in.Path, byte(in.Version))
		if err != nil {
			return nil, err
		}
	}
	return &ChunkResponse{Data: data}, nil
}

// GetChunkWithCheck implements PlukeServer
func (s *Server) GetChunkWithCheck(_ context.Context, in *ChunkRequestWithCheck) (*ChunkResponse, error) {
	if ok, err := s.checkAuth(in.Auth); !ok {
		logrus.Error(err)
		return nil, err
	}

	getData := func(path string, version byte) ([]byte, error) {
		reader, err := plukio.GetChunk(path, version)
		if err != nil {
			logrus.Error(err)
			return nil, err
		}
		bt := bytes.NewBuffer([]byte{})
		io.Copy(bt, reader)
		_ = reader.Close()
		return bt.Bytes(), nil
	}

	data, err := getData(in.Path, byte(in.Version))
	if err != nil {
		return nil, err
	}

	if len(data) == 0 || int64(len(data)) != in.Size {
		logrus.Warningf("Got chunk size %v/%v for %v, re-requesting", len(data), in.Size, in.Path)
		os.Remove(in.Path)
		data, err = getData(in.Path, byte(in.Version))
		if err != nil {
			return nil, err
		}
	}
	return &ChunkResponse{Data: data}, nil
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
