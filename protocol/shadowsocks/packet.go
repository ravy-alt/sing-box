package shadowsocks

import (
	"net"

	"github.com/sagernet/sing/common/buf"
	N "github.com/sagernet/sing/common/network"
)

type outboundPacketConn struct {
	N.NetPacketConn
}

func (c *outboundPacketConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	// Read and authenticate the whole encrypted datagram before applying the
	// caller's plaintext buffer limit. Truncating ciphertext drops even a
	// fitting plaintext reply when its salt/header/tag exceed that limit.
	buffer := buf.NewSize(65535)
	defer buffer.Release()
	destination, err := c.ReadPacket(buffer)
	if err != nil {
		return 0, nil, err
	}
	return copy(p, buffer.Bytes()), destination.UDPAddr(), nil
}

func (c *outboundPacketConn) Upstream() any {
	return c.NetPacketConn
}
