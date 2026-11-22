package v2raygrpc

import (
	"context"
	"fmt"
	"net"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/common"

	"google.golang.org/grpc/credentials"
)

// AuthInfo belongs to the exact HTTP/2 transport selected by gRPC, including
// reused connections. Socket addresses cannot identify multiplexed transports.
type tracedCredentials struct {
	credentials.TransportCredentials
}

func (c *tracedCredentials) ClientHandshake(ctx context.Context, authority string, raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn, info, err := c.TransportCredentials.ClientHandshake(ctx, authority, raw)
	if err != nil {
		return conn, info, err
	}
	if carrier, ok := common.Cast[adapter.OutboundTraceCarrier](raw); ok {
		info = &tracedAuthInfo{AuthInfo: info, trace: carrier.OutboundTrace()}
	}
	return conn, info, nil
}

func (c *tracedCredentials) Clone() credentials.TransportCredentials {
	return &tracedCredentials{c.TransportCredentials.Clone()}
}

type tracedAuthInfo struct {
	credentials.AuthInfo
	trace *adapter.OutboundTrace
}

func (i *tracedAuthInfo) GetCommonAuthInfo() credentials.CommonAuthInfo {
	if info, ok := i.AuthInfo.(interface {
		GetCommonAuthInfo() credentials.CommonAuthInfo
	}); ok {
		return info.GetCommonAuthInfo()
	}
	return credentials.CommonAuthInfo{}
}

func (i *tracedAuthInfo) ValidateAuthority(authority string) error {
	if validator, ok := i.AuthInfo.(credentials.AuthorityValidator); ok {
		return validator.ValidateAuthority(authority)
	}
	return fmt.Errorf("credentials type %q does not support authority overrides", i.AuthType())
}
