package kubeadm

import (
	"strings"
	"testing"
)

func TestParsePrimaryIP(t *testing.T) {
	tests := []struct {
		name    string
		output  string
		want    string
		wantErr bool
	}{
		{
			name:   "route source IPv4",
			output: "1.1.1.1 via 192.168.1.1 dev eth0 src 192.168.1.100 uid 1000",
			want:   "192.168.1.100",
		},
		{
			name:    "missing source",
			output:  "1.1.1.1 via 192.168.1.1 dev eth0",
			wantErr: true,
		},
		{
			name:    "invalid source",
			output:  "1.1.1.1 dev eth0 src not-an-ip",
			wantErr: true,
		},
		{
			name:    "IPv6 source",
			output:  "1.1.1.1 dev eth0 src 2001:db8::1",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parsePrimaryIP(tt.output)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parsePrimaryIP() error = %v, wantErr %t", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("parsePrimaryIP() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestFlannelIfaceStep(t *testing.T) {
	novpn := flannelIfaceStep(true)
	if strings.Contains(novpn, "wg0") {
		t.Errorf("VPN disabled: flannel must not be pinned to wg0, got %q", novpn)
	}
	if !strings.Contains(novpn, "--iface=$(FLANNEL_NODE_IP)") || !strings.Contains(novpn, "status.hostIP") {
		t.Errorf("VPN disabled: flannel must be pinned to the node IP via the downward API, got %q", novpn)
	}
	if got := flannelIfaceStep(false); !strings.Contains(got, "--iface=wg0") || strings.Contains(got, "FLANNEL_NODE_IP") {
		t.Errorf("VPN enabled: expected --iface=wg0 pinning only, got %q", got)
	}
}
