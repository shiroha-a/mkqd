package safedial

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsPublic(t *testing.T) {
	cases := []struct {
		addr   string
		public bool
		why    string
	}{
		{"1.1.1.1", true, "ordinary public v4"},
		{"93.184.216.34", true, "ordinary public v4"},
		{"2606:4700::1111", true, "ordinary public v6"},

		{"0.0.0.0", false, "unspecified"},
		{"::", false, "unspecified v6"},
		{"127.0.0.1", false, "loopback"},
		{"127.1.2.3", false, "loopback range, not just .0.1"},
		{"::1", false, "loopback v6"},
		{"10.0.0.1", false, "private"},
		{"172.16.0.1", false, "private"},
		{"172.31.255.255", false, "private upper bound"},
		{"192.168.1.1", false, "private"},
		{"fd00::1", false, "unique local v6"},
		{"169.254.169.254", false, "link-local: the cloud metadata endpoint"},
		{"fe80::1", false, "link-local v6"},
		{"224.0.0.1", false, "multicast"},
		{"ff02::1", false, "link-local multicast v6"},
		{"100.64.0.1", false, "carrier-grade NAT"},
		{"192.0.0.1", false, "IETF protocol assignments"},
		{"192.0.2.1", false, "documentation"},
		{"198.18.0.1", false, "benchmarking"},
		{"198.51.100.1", false, "documentation"},
		{"203.0.113.1", false, "documentation"},
		{"240.0.0.1", false, "reserved"},
		{"255.255.255.255", false, "broadcast"},
		{"2001:db8::1", false, "documentation v6"},
		{"2001::1", false, "Teredo, embeds v4"},
		{"2002:7f00:1::", false, "6to4, embeds v4"},
		{"100::1", false, "discard-only v6"},

		// 4-in-6 と NAT64 は、公開アドレスの見た目で private を指せる。
		{"::ffff:127.0.0.1", false, "IPv4-mapped loopback"},
		{"::ffff:10.0.0.1", false, "IPv4-mapped private"},
		{"::ffff:1.1.1.1", true, "IPv4-mapped public"},
		{"64:ff9b::7f00:1", false, "NAT64 wrapping 127.0.0.1"},
		{"64:ff9b::a00:1", false, "NAT64 wrapping 10.0.0.1"},
		{"64:ff9b::101:101", true, "NAT64 wrapping 1.1.1.1"},
	}

	for _, tc := range cases {
		t.Run(tc.addr, func(t *testing.T) {
			ip, err := netip.ParseAddr(tc.addr)
			require.NoError(t, err)
			require.Equal(t, tc.public, IsPublic(ip), tc.why)
		})
	}
}

func TestIsPublic_InvalidAddrIsNotPublic(t *testing.T) {
	require.False(t, IsPublic(netip.Addr{}))
}

func TestCheckAddr(t *testing.T) {
	require.NoError(t, CheckAddr("1.1.1.1:443"))
	require.NoError(t, CheckAddr("[2606:4700::1111]:443"))

	require.ErrorIs(t, CheckAddr("127.0.0.1:80"), ErrBlocked)
	require.ErrorIs(t, CheckAddr("169.254.169.254:80"), ErrBlocked)
	// ポートがない、ホスト名のまま、といった想定外の入力は通さず塞ぐ。
	require.ErrorIs(t, CheckAddr("1.1.1.1"), ErrBlocked)
	require.ErrorIs(t, CheckAddr("example.com:443"), ErrBlocked)
}

func TestNewDialer_GuardRejectsLoopback(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()

	guarded := NewDialer(false)
	_, err = guarded.DialContext(context.Background(), "tcp", ln.Addr().String())
	require.Error(t, err)
	require.ErrorIs(t, err, ErrBlocked)
}

func TestNewDialer_AllowPrivateReachesLoopback(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()
	go func() {
		conn, aerr := ln.Accept()
		if aerr == nil {
			_ = conn.Close()
		}
	}()

	open := NewDialer(true)
	conn, err := open.DialContext(context.Background(), "tcp", ln.Addr().String())
	require.NoError(t, err)
	require.NoError(t, conn.Close())
}

// The guard runs in Control, which sees the address the dialer is about
// to connect to. A hostname that resolves to a blocked address is
// therefore rejected at connect time, not at resolution time, which is
// what closes the DNS rebinding window.
func TestNewDialer_GuardRejectsHostnameResolvingToLoopback(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()

	_, port, err := net.SplitHostPort(ln.Addr().String())
	require.NoError(t, err)

	guarded := NewDialer(false)
	_, err = guarded.DialContext(context.Background(), "tcp", net.JoinHostPort("localhost", port))
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrBlocked), "got %v", err)
}
