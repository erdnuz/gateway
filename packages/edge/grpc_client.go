package edge

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"gateway/packages/common/types"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

type QuotaLeaseRequester interface {
	RequestQuotaLease(ctx context.Context, in *types.QuotaLeaseRequest) (*types.QuotaLeaseResponse, error)
	Close() error
}

type grpcQuotaLeaseClient struct {
	conn   *grpc.ClientConn
	client types.QuotaLeaseServiceClient
}

func NewGRPCQuotaLeaseClient(addr, serverName, certFile, keyFile, caFile string) (QuotaLeaseRequester, error) {
	tlsCfg, err := loadClientTLSConfig(serverName, certFile, keyFile, caFile)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.Dial(
		addr,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, err
	}
	return &grpcQuotaLeaseClient{conn: conn, client: types.NewQuotaLeaseServiceClient(conn)}, nil
}

func (c *grpcQuotaLeaseClient) RequestQuotaLease(ctx context.Context, in *types.QuotaLeaseRequest) (*types.QuotaLeaseResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return c.client.RequestQuotaLease(ctx, in)
}

func (c *grpcQuotaLeaseClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func loadClientTLSConfig(serverName, certFile, keyFile, caFile string) (*tls.Config, error) {
	if serverName == "" {
		return nil, fmt.Errorf("server name is required for TLS verification")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load client certificate: %w", err)
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read client CA: %w", err)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("parse client CA PEM")
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caPool,
		ServerName:   serverName,
		MinVersion:   tls.VersionTLS13,
	}, nil
}
