package zk

// Permission represents ZooKeeper ACL permission bits.
type Permission uint8

const (
	// PermRead allows reading node data and listing children.
	PermRead Permission = 1 << iota // 1

	// PermWrite allows setting node data.
	PermWrite // 2

	// PermCreate allows creating child nodes.
	PermCreate // 4

	// PermDelete allows deleting child nodes.
	PermDelete // 8

	// PermAdmin allows setting ACLs.
	PermAdmin // 16

	// PermAll grants all permissions.
	PermAll Permission = PermRead | PermWrite | PermCreate | PermDelete | PermAdmin // 31
)

// Has checks if the permission set includes the given permission.
func (p Permission) Has(perm Permission) bool {
	return p&perm == perm
}

// String returns a string representation of permissions.
func (p Permission) String() string {
	var s string
	if p.Has(PermRead) {
		s += "r"
	} else {
		s += "-"
	}
	if p.Has(PermWrite) {
		s += "w"
	} else {
		s += "-"
	}
	if p.Has(PermCreate) {
		s += "c"
	} else {
		s += "-"
	}
	if p.Has(PermDelete) {
		s += "d"
	} else {
		s += "-"
	}
	if p.Has(PermAdmin) {
		s += "a"
	} else {
		s += "-"
	}
	return s
}

// ACL represents an Access Control List entry.
type ACL struct {
	Perms  Permission
	Scheme string
	ID     string
}

// Common ACL schemes
const (
	SchemeWorld  = "world"
	SchemeAuth   = "auth"
	SchemeDigest = "digest"
	SchemeIP     = "ip"
	SchemeSASL   = "sasl"
)

// Common ACL identities
const (
	IDAnyone = "anyone"
)

// WorldACL returns the default open ACL (world:anyone with all permissions).
func WorldACL() []ACL {
	return []ACL{
		{
			Perms:  PermAll,
			Scheme: SchemeWorld,
			ID:     IDAnyone,
		},
	}
}

// ReadOnlyACL returns a read-only world ACL.
func ReadOnlyACL() []ACL {
	return []ACL{
		{
			Perms:  PermRead,
			Scheme: SchemeWorld,
			ID:     IDAnyone,
		},
	}
}

// DigestACL creates an ACL for digest authentication.
func DigestACL(perms Permission, user, encodedPassword string) ACL {
	return ACL{
		Perms:  perms,
		Scheme: SchemeDigest,
		ID:     user + ":" + encodedPassword,
	}
}

// IPACL creates an ACL for IP-based access control.
func IPACL(perms Permission, ipOrCIDR string) ACL {
	return ACL{
		Perms:  perms,
		Scheme: SchemeIP,
		ID:     ipOrCIDR,
	}
}

// Clone returns a deep copy of the ACL slice.
func CloneACLs(acls []ACL) []ACL {
	if acls == nil {
		return nil
	}
	result := make([]ACL, len(acls))
	copy(result, acls)
	return result
}
