package main

import "testing"

func TestMultiLimitAdmittedBounds(t *testing.T) {
	lo, hi := multiLimitAdmittedBounds(300, 2)
	if lo != 296 || hi != 302 {
		t.Fatalf("unexpected bounds: got [%d,%d], want [296,302]", lo, hi)
	}
}
