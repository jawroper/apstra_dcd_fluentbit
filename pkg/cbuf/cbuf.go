// Package cbuf provides the one piece of cgo-unsafe-pointer plumbing this
// plugin needs to get right: handing a Go-built []byte to Fluent Bit's C
// engine in a way that survives past the returning Go function call.
//
// This is its own package (rather than living directly in cmd/plugin) partly
// for testability: cmd/plugin/main.go has `//export` cgo directives, and Go
// cannot run `go test` against a package containing those (it conflicts with
// the c-shared build mode they require). Separately, Go also flatly
// disallows `import "C"` in any _test.go file in any package (a long-standing
// restriction — see golang/go#4030) — which is why this package's own test
// file never imports "C" itself; it calls BytesAt below instead of using
// C.GoBytes directly.
package cbuf

/*
#include <stdlib.h>
#include <string.h>
*/
import "C"
import "unsafe"

// ToCBuffer copies a Go-owned byte slice into a freshly C.malloc'd buffer and
// returns a pointer to it, suitable for handing back to Fluent Bit's C
// engine via FLBPluginInputCallback(Ctx)'s *data out-parameter. Returns nil
// for empty input.
//
// This is NOT optional, and getting it wrong doesn't fail loudly at the call
// site — it crashes Fluent Bit itself, asynchronously, sometime after this
// function returns. Fluent Bit's C engine reads from *data after the Go
// callback returns, but a pointer into Go-managed memory (e.g. &b[0]) is
// only valid for as long as Go itself can see a live reference to it — once
// the Go function returns with no remaining Go-side reference, the garbage
// collector is free to move or reclaim that memory at any time. This was a
// real production bug: it crashed Fluent Bit with SIGSEGV the moment enough
// real data flowed through to make the GC timing matter — every earlier test
// run had decoded zero records, so this exact handoff code path had never
// actually executed until a separate, unrelated decoding bug was fixed.
//
// The fix, confirmed against fluent-bit-go's own reference input plugin
// examples ("this passing pointer should be allocated by C style
// allocation"), is to allocate with C.malloc instead, which the Go GC never
// touches. The caller is expected to free the returned buffer once Fluent
// Bit signals it's done with it, via FLBPluginInputCleanupCallback.
func ToCBuffer(b []byte) unsafe.Pointer {
	if len(b) == 0 {
		return nil
	}
	cBuf := C.malloc(C.size_t(len(b)))
	if cBuf == nil {
		return nil // out of memory; caller must handle
	}
	C.memcpy(cBuf, unsafe.Pointer(&b[0]), C.size_t(len(b)))
	return cBuf
}

// Free releases a buffer previously returned by ToCBuffer. Safe to call with
// the nil pointer Fluent Bit may pass through FLBPluginInputCleanupCallback
// in edge cases (C.free(nil) is a defined no-op).
func Free(p unsafe.Pointer) {
	C.free(p)
}

// BytesAt copies n bytes starting at p into a new Go []byte. Exists so test
// code (which cannot itself use `import "C"` — see the package doc comment)
// can read back a C buffer's contents to verify ToCBuffer's copy.
func BytesAt(p unsafe.Pointer, n int) []byte {
	return C.GoBytes(p, C.int(n))
}
