package cfg

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestNormalizeDNSServer(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "empty", input: "", want: ""},
		{name: "IPv4 default port", input: "10.0.0.53", want: "10.0.0.53:53"},
		{name: "IPv4 custom port", input: "10.0.0.53:5353", want: "10.0.0.53:5353"},
		{name: "IPv6 default port", input: "2001:db8::53", want: "[2001:db8::53]:53"},
		{name: "IPv6 custom port", input: "[2001:db8::53]:5353", want: "[2001:db8::53]:5353"},
		{name: "IPv6 zone", input: "fe80::53%en0", want: "[fe80::53%en0]:53"},
		{name: "hostname", input: "dns.example.com", wantErr: true},
		{name: "invalid port", input: "10.0.0.53:dns", wantErr: true},
		{name: "zero port", input: "10.0.0.53:0", wantErr: true},
		{name: "missing IPv6 bracket", input: "[2001:db8::53", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeDNSServer(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("normalizeDNSServer(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("normalizeDNSServer(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestConfigureDNSServer(t *testing.T) {
	dnsConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer dnsConn.Close()

	if err := ConfigureDNSServer(dnsConn.LocalAddr().String()); err != nil {
		t.Fatal(err)
	}
	defer ConfigureDNSServer("")

	queryReceived := make(chan struct{})
	go func() {
		buf := make([]byte, 512)
		if _, _, err := dnsConn.ReadFrom(buf); err == nil {
			close(queryReceived)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := GetHTTPTransport().DialContext(ctx, "tcp", "dns-server-test.invalid:80")
	if conn != nil {
		conn.Close()
	}
	if err == nil {
		t.Fatal("expected lookup or connection error")
	}

	select {
	case <-queryReceived:
	case <-time.After(time.Second):
		t.Fatal("custom DNS server did not receive a query")
	}
}
