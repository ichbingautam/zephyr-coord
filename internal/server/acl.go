// Package server - ACL authentication providers.
package server

import (
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"net"
	"strings"
	"sync"

	"github.com/ichbingautam/zephyr-coord/pkg/zk"
)

// ACL errors
var (
	ErrACLNoAuth           = errors.New("not authenticated")
	ErrACLInvalidAuth      = errors.New("invalid authentication")
	ErrACLPermissionDenied = errors.New("permission denied")
	ErrACLInvalid          = errors.New("invalid ACL")
)

// AuthInfo holds authentication information for a session.
type AuthInfo struct {
	Scheme string
	ID     string
}

// AuthProvider authenticates clients and checks permissions.
type AuthProvider interface {
	// Scheme returns the authentication scheme name.
	Scheme() string

	// Authenticate validates credentials and returns the identity.
	Authenticate(credentials []byte) (string, error)

	// Matches checks if the auth info matches the ACL entry.
	Matches(auth AuthInfo, acl zk.ACL) bool
}

// ACLManager manages authentication and authorization.
type ACLManager struct {
	providers map[string]AuthProvider
	mu        sync.RWMutex
}

// NewACLManager creates a new ACL manager.
func NewACLManager() *ACLManager {
	m := &ACLManager{
		providers: make(map[string]AuthProvider),
	}

	// Register default providers
	m.RegisterProvider(&WorldProvider{})
	m.RegisterProvider(&DigestProvider{})
	m.RegisterProvider(&IPProvider{})

	return m
}

// RegisterProvider adds an authentication provider.
func (m *ACLManager) RegisterProvider(p AuthProvider) {
	m.mu.Lock()
	m.providers[p.Scheme()] = p
	m.mu.Unlock()
}

// GetProvider returns the provider for a scheme.
func (m *ACLManager) GetProvider(scheme string) (AuthProvider, bool) {
	m.mu.RLock()
	p, ok := m.providers[scheme]
	m.mu.RUnlock()
	return p, ok
}

// CheckPermission verifies if the session has permission for the operation.
func (m *ACLManager) CheckPermission(
	acls []zk.ACL,
	perm zk.Permission,
	auths []AuthInfo,
	clientIP net.IP,
) error {
	// Empty ACL list = open access (should not happen in practice)
	if len(acls) == 0 {
		return nil
	}

	for _, acl := range acls {
		if !acl.Perms.Has(perm) {
			continue
		}

		provider, ok := m.GetProvider(acl.Scheme)
		if !ok {
			continue
		}

		// Check world scheme
		if acl.Scheme == zk.SchemeWorld && acl.ID == zk.IDAnyone {
			return nil
		}

		// Check IP scheme
		if acl.Scheme == zk.SchemeIP {
			if ipProvider, ok := provider.(*IPProvider); ok {
				if ipProvider.MatchesIP(clientIP, acl.ID) {
					return nil
				}
			}
			continue
		}

		// Check authenticated schemes
		for _, auth := range auths {
			if provider.Matches(auth, acl) {
				return nil
			}
		}
	}

	return ErrACLPermissionDenied
}

// ValidateACL checks if an ACL list is valid.
func (m *ACLManager) ValidateACL(acls []zk.ACL) error {
	if len(acls) == 0 {
		return ErrACLInvalid
	}

	for _, acl := range acls {
		if acl.Perms == 0 {
			return ErrACLInvalid
		}

		if acl.Scheme == "" || acl.ID == "" {
			return ErrACLInvalid
		}

		if _, ok := m.GetProvider(acl.Scheme); !ok {
			return ErrACLInvalid
		}
	}

	return nil
}

// WorldProvider handles world:anyone authentication.
type WorldProvider struct{}

func (p *WorldProvider) Scheme() string { return zk.SchemeWorld }

func (p *WorldProvider) Authenticate(credentials []byte) (string, error) {
	return zk.IDAnyone, nil
}

func (p *WorldProvider) Matches(auth AuthInfo, acl zk.ACL) bool {
	return acl.Scheme == zk.SchemeWorld && acl.ID == zk.IDAnyone
}

// DigestProvider handles digest (username:password) authentication.
type DigestProvider struct{}

func (p *DigestProvider) Scheme() string { return zk.SchemeDigest }

func (p *DigestProvider) Authenticate(credentials []byte) (string, error) {
	// Credentials format: username:password
	parts := strings.SplitN(string(credentials), ":", 2)
	if len(parts) != 2 {
		return "", ErrACLInvalidAuth
	}

	username := parts[0]
	password := parts[1]

	// Hash password: base64(sha1(username:password))
	hash := sha1.Sum([]byte(username + ":" + password))
	encoded := base64.StdEncoding.EncodeToString(hash[:])

	return username + ":" + encoded, nil
}

func (p *DigestProvider) Matches(auth AuthInfo, acl zk.ACL) bool {
	if auth.Scheme != zk.SchemeDigest || acl.Scheme != zk.SchemeDigest {
		return false
	}
	return auth.ID == acl.ID
}

// EncodeDigest encodes a password for digest authentication.
func EncodeDigest(username, password string) string {
	hash := sha1.Sum([]byte(username + ":" + password))
	return base64.StdEncoding.EncodeToString(hash[:])
}

// IPProvider handles IP-based authentication.
type IPProvider struct{}

func (p *IPProvider) Scheme() string { return zk.SchemeIP }

func (p *IPProvider) Authenticate(credentials []byte) (string, error) {
	// IP auth doesn't use credentials, it uses client IP
	return "", nil
}

func (p *IPProvider) Matches(auth AuthInfo, acl zk.ACL) bool {
	// IP matching is done via MatchesIP
	return false
}

// MatchesIP checks if the client IP matches the ACL pattern.
func (p *IPProvider) MatchesIP(clientIP net.IP, pattern string) bool {
	if clientIP == nil {
		return false
	}

	// Check for CIDR notation
	if strings.Contains(pattern, "/") {
		_, network, err := net.ParseCIDR(pattern)
		if err != nil {
			return false
		}
		return network.Contains(clientIP)
	}

	// Exact IP match
	patternIP := net.ParseIP(pattern)
	if patternIP == nil {
		return false
	}

	return clientIP.Equal(patternIP)
}
