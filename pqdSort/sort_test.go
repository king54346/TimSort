package pqdSort

import (
	"fmt"
	"math"
	"testing"
)

var float64s = [...]float64{74.3, 59.0, math.Inf(1), 238.2, -784.0, 2.3, math.Inf(-1), 9845.768, -959.7485, 905, 7.8, 7.8, 74.3, 59.0, math.Inf(1), 238.2, -784.0, 2.3}

func TestName(t *testing.T) {
	data := float64s[:]
	Sort(data)
	if !IsSorted(data) {
		t.Errorf("sorted %v", float64s)
		t.Errorf("   got %v", data)
	}
	fmt.Println(data)
}
