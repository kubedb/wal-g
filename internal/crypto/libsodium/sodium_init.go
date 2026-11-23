//go:build libsodium
// +build libsodium

package libsodium

// #cgo CFLAGS: -I../../../tmp/libsodium/include
// #cgo LDFLAGS: -L../../../tmp/libsodium/lib -lsodium
// #include <sodium.h>
import "C"

// libsodium should always be initialized
func init() {
	C.sodium_init()
}
