package region

import (
	"fmt"
	"testing"
)

func TestBaseAllocationsSumToTheQuota(t *testing.T) {
	floors := map[string]float64{"eu": 0.3, "us": 0.25, "ap": 0.1}
	homes := map[string]int{}
	for i := 0; i < 300; i++ {
		subject := fmt.Sprintf("key%d", i)
		for _, q := range []int64{1, 7, 100, 999} {
			var sum int64
			for name := range floors {
				sum += Base(q, floors, name, subject)
			}
			if sum != q {
				t.Fatalf("%s q=%d: allocations sum to %d", subject, q, sum)
			}
		}
		homes[Home(floors, subject)]++
	}
	for name, n := range homes {
		if n < 60 {
			t.Fatalf("home regions are unbalanced: %v (%s)", homes, name)
		}
	}
	if Base(100, floors, "nowhere", "k") != 0 {
		t.Fatal("an unknown region was allocated quota")
	}
}
