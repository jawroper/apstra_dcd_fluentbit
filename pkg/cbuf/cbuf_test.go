package cbuf

import (
	"testing"
	"unsafe"
)

// TestToCBuffer_CopiesCorrectly verifies the byte content survives the
// Go-slice → C-buffer copy intact.
func TestToCBuffer_CopiesCorrectly(t *testing.T) {
	want := []byte("hello from go, this is a test payload that mimics an encoded msgpack record batch")
	cBuf := ToCBuffer(want)
	if cBuf == nil {
		t.Fatal("ToCBuffer returned nil for non-empty input")
	}
	defer Free(cBuf)

	got := BytesAt(cBuf, len(want))
	if string(got) != string(want) {
		t.Errorf("copied bytes don't match: want %q, got %q", want, got)
	}
}

// TestToCBuffer_DoesNotAliasGoSlice is the actual regression guard for the
// production crash: the returned pointer must NOT point into the original Go
// slice's backing array. If it did, we'd be back to handing Fluent Bit's C
// engine a pointer the Go garbage collector is free to invalidate after this
// function returns — exactly the bug that crashed Fluent Bit with SIGSEGV
// once real decoded records started flowing.
func TestToCBuffer_DoesNotAliasGoSlice(t *testing.T) {
	// Values 0..199, deliberately never reaching 0xFF, so the mutation
	// sentinel below can never collide with a legitimately-original byte.
	goSlice := make([]byte, 200)
	for i := range goSlice {
		goSlice[i] = byte(i)
	}

	cBuf := ToCBuffer(goSlice)
	if cBuf == nil {
		t.Fatal("ToCBuffer returned nil for non-empty input")
	}
	defer Free(cBuf)

	goPtr := uintptr(unsafe.Pointer(&goSlice[0]))
	cPtr := uintptr(cBuf)

	if cPtr == goPtr {
		t.Fatal("ToCBuffer returned a pointer into the original Go slice's backing array — " +
			"this is the exact bug that crashed Fluent Bit; the buffer must be a separate C.malloc allocation")
	}

	// Mutate the Go slice after the copy; the C buffer's content must be
	// unaffected, proving it's a true independent copy, not an alias.
	for i := range goSlice {
		goSlice[i] = 0xFF
	}
	got := BytesAt(cBuf, len(goSlice))
	for i, b := range got {
		if b == 0xFF {
			t.Fatalf("C buffer byte %d changed after mutating the Go slice — buffer is aliasing Go memory, not an independent copy", i)
		}
	}
}

func TestToCBuffer_EmptyInputReturnsNil(t *testing.T) {
	if got := ToCBuffer(nil); got != nil {
		t.Errorf("expected nil for empty input, got %v", got)
	}
	if got := ToCBuffer([]byte{}); got != nil {
		t.Errorf("expected nil for empty input, got %v", got)
	}
}

func TestFree_NilIsSafe(t *testing.T) {
	// Must not panic/crash — Fluent Bit could plausibly call the cleanup
	// callback with a nil pointer in some edge case.
	Free(nil)
}
