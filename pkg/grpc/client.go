package grpc

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/kuberlab/pluk/pkg/plukclient"
	"google.golang.org/grpc"
)

const (
	timeout = time.Second * 10
)

type Client struct {
	auth     *Auth
	conn     *grpc.ClientConn
	internal PlukeClient
}

func NewClient(address string, opts *plukclient.AuthOpts) (*Client, error) {
	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("did not connect: %v", err)
	} else {
		// Check port
		cn, err := net.Dial("tcp", address)
		if err != nil {
			return nil, err
		}
		_ = cn.Close()
	}

	logrus.Infof("Connected to grpc server at %v.", address)

	return &Client{
		conn:     conn,
		internal: NewPlukeClient(conn),
		auth:     &Auth{Token: opts.Token, Workspace: opts.Workspace, Secret: opts.Secret},
	}, nil
}

func (c *Client) GetChunk(path string, version byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	resp, err := c.internal.GetChunk(
		ctx,
		&ChunkRequest{
			Path:    path,
			Version: int32(version),
			Auth:    &Auth{},
		},
	)
	if err != nil {
		return nil, err
	}
	return resp.Data, nil
}
