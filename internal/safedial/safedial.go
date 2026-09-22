// Package safedial builds net.Dialers that refuse to connect anywhere
// except the public internet.
//
// It exists for the executors whose destination comes out of a job
// payload: without a guard, anyone who can enqueue a job can make the
// worker fetch a cloud metadata endpoint or probe the private network
// it happens to sit in.
//
// 検査は名前解決の後に行う。ホスト名を先に解決して検査し、その後で
// Dial すると、2 回目の解決で別の IP が返る DNS rebinding をすり抜ける。
// net.Dialer.Control は接続直前に実際の宛先アドレスを受け取るので、
// ここで弾けば入れ替わりの隙がない。
package safedial

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"syscall"
	"time"
)

// ErrBlocked reports a destination outside the public internet.
var ErrBlocked = errors.New("safedial: destination is not a public address")

// Default dial timings. They bound the connect phase only; the overall
// request deadline belongs to the caller's context.
const (
	defaultTimeout   = 10 * time.Second
	defaultKeepAlive = 30 * time.Second
)

// NewDialer returns a dialer for an http.Transport. When allowPrivate
// is true the guard is off and the dialer behaves like a plain
// net.Dialer, which is what a config-supplied destination such as
// "http://127.0.0.1:3000" needs.
func NewDialer(allowPrivate bool) *net.Dialer {
	d := &net.Dialer{Timeout: defaultTimeout, KeepAlive: defaultKeepAlive}
	if !allowPrivate {
		d.Control = control
	}
	return d
}

func control(_, address string, _ syscall.RawConn) error {
	return CheckAddr(address)
}

// CheckAddr reports whether a resolved "ip:port" may be connected to.
func CheckAddr(address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("%w: cannot parse address %q: %v", ErrBlocked, address, err)
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		// Control はすでに解決済みのアドレスを渡してくる。ここに来たなら
		// 想定外なので、通すのではなく塞ぐ。
		return fmt.Errorf("%w: %q is not an IP address", ErrBlocked, host)
	}
	if !IsPublic(ip) {
		return fmt.Errorf("%w: %s", ErrBlocked, ip)
	}
	return nil
}

// v4EmbeddingPrefixes are the IPv6 ranges that carry an IPv4 address in
// their low 32 bits. An address inside one of them can name a private
// v4 destination while looking like a public v6 one, so the embedded
// address has to be checked rather than the wrapper.
//
// 6to4 (2002::/16) と Teredo (2001::/32) も v4 を内包するが、そちらは
// 範囲ごと blockedPrefixes で塞いでいるので展開の必要がない。
var v4EmbeddingPrefixes = []netip.Prefix{
	netip.MustParsePrefix("64:ff9b::/96"),   // RFC 6052 well-known NAT64
	netip.MustParsePrefix("64:ff9b:1::/48"), // RFC 8215 local-use NAT64
	netip.MustParsePrefix("::/96"),          // RFC 4291 IPv4-compatible (deprecated)
}

// blockedPrefixes covers the ranges the netip.Addr predicates do not.
var blockedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),       // RFC 1122 "this network"
	netip.MustParsePrefix("100.64.0.0/10"),   // RFC 6598 carrier-grade NAT
	netip.MustParsePrefix("192.0.0.0/24"),    // RFC 6890 IETF protocol assignments
	netip.MustParsePrefix("192.88.99.0/24"),  // RFC 7526 deprecated 6to4 relay anycast
	netip.MustParsePrefix("192.0.2.0/24"),    // RFC 5737 documentation
	netip.MustParsePrefix("198.18.0.0/15"),   // RFC 2544 benchmarking
	netip.MustParsePrefix("198.51.100.0/24"), // RFC 5737 documentation
	netip.MustParsePrefix("203.0.113.0/24"),  // RFC 5737 documentation
	netip.MustParsePrefix("240.0.0.0/4"),     // RFC 1112 reserved, includes 255.255.255.255
	netip.MustParsePrefix("100::/64"),        // RFC 6666 discard-only
	netip.MustParsePrefix("2001::/32"),       // RFC 4380 Teredo, embeds IPv4
	netip.MustParsePrefix("2001:20::/28"),    // RFC 7343 ORCHIDv2, not routable
	netip.MustParsePrefix("2001:db8::/32"),   // RFC 3849 documentation
	netip.MustParsePrefix("2002::/16"),       // RFC 3056 6to4, embeds IPv4
}

// IsPublic reports whether ip is an address on the public internet.
func IsPublic(ip netip.Addr) bool {
	if !ip.IsValid() {
		return false
	}
	ip = ip.Unmap()

	// IPv4 を内側に抱える表現は、埋め込まれた v4 アドレスまで見ないと
	// private 宛を見逃す。
	for _, p := range v4EmbeddingPrefixes {
		if p.Contains(ip) {
			b := ip.As16()
			return IsPublic(netip.AddrFrom4([4]byte{b[12], b[13], b[14], b[15]}))
		}
	}

	if ip.IsUnspecified() ||
		ip.IsLoopback() ||
		ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsInterfaceLocalMulticast() ||
		ip.IsMulticast() {
		return false
	}
	for _, p := range blockedPrefixes {
		if p.Contains(ip) {
			return false
		}
	}
	return true
}
