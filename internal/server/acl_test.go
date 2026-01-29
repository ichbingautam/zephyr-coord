package server

import (
	"net"
	"testing"

	"github.com/ichbingautam/zephyr-coord/pkg/zk"
)

func TestACLManager_WorldProvider(t *testing.T) {
	m := NewACLManager()

	// World:anyone should always allow
	acls := zk.WorldACL()
	auths := []AuthInfo{}

	err := m.CheckPermission(acls, zk.PermRead, auths, nil)
	if err != nil {
		t.Errorf("expected no error for world:anyone, got %v", err)
	}

	err = m.CheckPermission(acls, zk.PermWrite, auths, nil)
	if err != nil {
		t.Errorf("expected no error for world:anyone write, got %v", err)
	}
}

func TestACLManager_DigestProvider(t *testing.T) {
	m := NewACLManager()

	password := EncodeDigest("user", "pass")
	acls := []zk.ACL{
		{Perms: zk.PermAll, Scheme: zk.SchemeDigest, ID: "user:" + password},
	}

	// Without auth, should fail
	err := m.CheckPermission(acls, zk.PermRead, nil, nil)
	if err != ErrACLPermissionDenied {
		t.Errorf("expected permission denied without auth, got %v", err)
	}

	// With wrong auth, should fail
	wrongAuth := []AuthInfo{{Scheme: zk.SchemeDigest, ID: "user:wrongpassword"}}
	err = m.CheckPermission(acls, zk.PermRead, wrongAuth, nil)
	if err != ErrACLPermissionDenied {
		t.Errorf("expected permission denied with wrong auth, got %v", err)
	}

	// With correct auth, should pass
	correctAuth := []AuthInfo{{Scheme: zk.SchemeDigest, ID: "user:" + password}}
	err = m.CheckPermission(acls, zk.PermRead, correctAuth, nil)
	if err != nil {
		t.Errorf("expected no error with correct auth, got %v", err)
	}
}

func TestACLManager_IPProvider(t *testing.T) {
	m := NewACLManager()

	// Exact IP match
	acls := []zk.ACL{
		{Perms: zk.PermRead, Scheme: zk.SchemeIP, ID: "192.168.1.100"},
	}

	clientIP := net.ParseIP("192.168.1.100")
	err := m.CheckPermission(acls, zk.PermRead, nil, clientIP)
	if err != nil {
		t.Errorf("expected no error for matching IP, got %v", err)
	}

	wrongIP := net.ParseIP("192.168.1.101")
	err = m.CheckPermission(acls, zk.PermRead, nil, wrongIP)
	if err != ErrACLPermissionDenied {
		t.Errorf("expected permission denied for wrong IP, got %v", err)
	}
}

func TestACLManager_CIDR(t *testing.T) {
	m := NewACLManager()

	acls := []zk.ACL{
		{Perms: zk.PermAll, Scheme: zk.SchemeIP, ID: "10.0.0.0/8"},
	}

	// IP in range
	inRange := net.ParseIP("10.1.2.3")
	err := m.CheckPermission(acls, zk.PermRead, nil, inRange)
	if err != nil {
		t.Errorf("expected no error for IP in CIDR range, got %v", err)
	}

	// IP out of range
	outOfRange := net.ParseIP("192.168.1.1")
	err = m.CheckPermission(acls, zk.PermRead, nil, outOfRange)
	if err != ErrACLPermissionDenied {
		t.Errorf("expected permission denied for IP out of CIDR range, got %v", err)
	}
}

func TestACLManager_PermissionCheck(t *testing.T) {
	m := NewACLManager()

	// ACL with only read permission
	acls := zk.ReadOnlyACL()

	err := m.CheckPermission(acls, zk.PermRead, nil, nil)
	if err != nil {
		t.Errorf("expected read to succeed, got %v", err)
	}

	err = m.CheckPermission(acls, zk.PermWrite, nil, nil)
	if err != ErrACLPermissionDenied {
		t.Errorf("expected write to be denied, got %v", err)
	}
}

func TestACLManager_ValidateACL(t *testing.T) {
	m := NewACLManager()

	tests := []struct {
		name    string
		acls    []zk.ACL
		wantErr bool
	}{
		{
			name:    "valid world ACL",
			acls:    zk.WorldACL(),
			wantErr: false,
		},
		{
			name:    "empty ACL list",
			acls:    []zk.ACL{},
			wantErr: true,
		},
		{
			name:    "nil ACL list",
			acls:    nil,
			wantErr: true,
		},
		{
			name:    "zero permissions",
			acls:    []zk.ACL{{Perms: 0, Scheme: zk.SchemeWorld, ID: zk.IDAnyone}},
			wantErr: true,
		},
		{
			name:    "empty scheme",
			acls:    []zk.ACL{{Perms: zk.PermRead, Scheme: "", ID: zk.IDAnyone}},
			wantErr: true,
		},
		{
			name:    "unknown scheme",
			acls:    []zk.ACL{{Perms: zk.PermRead, Scheme: "unknown", ID: "test"}},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := m.ValidateACL(tc.acls)
			if (err != nil) != tc.wantErr {
				t.Errorf("ValidateACL() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestDigestProvider_Authenticate(t *testing.T) {
	p := &DigestProvider{}

	id, err := p.Authenticate([]byte("user:password"))
	if err != nil {
		t.Fatalf("authentication failed: %v", err)
	}

	// Should be user:base64(sha1(user:password))
	expected := "user:" + EncodeDigest("user", "password")
	if id != expected {
		t.Errorf("expected %s, got %s", expected, id)
	}
}

func TestDigestProvider_InvalidCredentials(t *testing.T) {
	p := &DigestProvider{}

	_, err := p.Authenticate([]byte("nocolon"))
	if err != ErrACLInvalidAuth {
		t.Errorf("expected ErrACLInvalidAuth, got %v", err)
	}
}

func TestIPProvider_MatchesIP(t *testing.T) {
	p := &IPProvider{}

	tests := []struct {
		clientIP string
		pattern  string
		expected bool
	}{
		{"192.168.1.1", "192.168.1.1", true},
		{"192.168.1.1", "192.168.1.2", false},
		{"192.168.1.50", "192.168.1.0/24", true},
		{"192.168.2.1", "192.168.1.0/24", false},
		{"10.0.0.1", "10.0.0.0/8", true},
		{"172.16.0.1", "10.0.0.0/8", false},
		{"::1", "::1", true},
		{"::1", "127.0.0.1", false},
	}

	for _, tc := range tests {
		ip := net.ParseIP(tc.clientIP)
		result := p.MatchesIP(ip, tc.pattern)
		if result != tc.expected {
			t.Errorf("MatchesIP(%s, %s) = %v, expected %v",
				tc.clientIP, tc.pattern, result, tc.expected)
		}
	}
}

func BenchmarkACLManager_CheckPermission(b *testing.B) {
	m := NewACLManager()
	acls := zk.WorldACL()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.CheckPermission(acls, zk.PermRead, nil, nil)
	}
}

func BenchmarkDigestProvider_Authenticate(b *testing.B) {
	p := &DigestProvider{}
	creds := []byte("username:password")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Authenticate(creds)
	}
}
