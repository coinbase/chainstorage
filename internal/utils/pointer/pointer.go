package pointer

func String(v string) *string {
	return &v
}

func StringDeref(p *string) string {
	if p == nil {
		return ""
	}

	return *p
}

func Int(v int) *int {
	return &v
}

func IntDeref(p *int) int {
	if p == nil {
		return 0
	}

	return *p
}

func Int32(v int32) *int32 {
	return &v
}

func Int32Deref(p *int32) int32 {
	if p == nil {
		return 0
	}

	return *p
}

func Uint32(v uint32) *uint32 {
	return &v
}

func Uint32Deref(p *uint32) uint32 {
	if p == nil {
		return 0
	}

	return *p
}

func Int64(v int64) *int64 {
	return &v
}

func Int64Deref(p *int64) int64 {
	if p == nil {
		return 0
	}

	return *p
}

func Uint64(v uint64) *uint64 {
	return &v
}

func Uint64Deref(p *uint64) uint64 {
	if p == nil {
		return 0
	}

	return *p
}

func Bool(v bool) *bool {
	return &v
}

func BoolDeref(p *bool) bool {
	if p == nil {
		return false
	}

	return *p
}
