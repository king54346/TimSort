package TimSort

import (
	"math/rand"
	"sort"
	"testing"
)

const N = 1000000

func makeRandomInts(n int) []int {
	rand.Seed(42)
	ints := make([]int, n)
	for i := 0; i < n; i++ {
		ints[i] = rand.Intn(n)
	}
	return ints
}
func makeSortedInts(n int) []int {
	ints := make([]int, n)
	for i := 0; i < n; i++ {
		ints[i] = i
	}
	return ints
}

func BenchmarkSortInts(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		ints := makeRandomInts(N)
		b.StartTimer()
		sort.Ints(ints)
	}
}
func BenchmarkTimSortInts(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		ints := makeRandomInts(N)
		b.StartTimer()
		Sort(ints)
	}
}

func BenchmarkParallelSortInts(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		ints := makeRandomInts(N)
		b.StartTimer()
		ParallelSort[int](ints)
	}
}

func TestIsSortedFunc(t *testing.T) {
	for i := 0; i < 100; i++ {
		ints := makeRandomInts(N)
		ParallelSort[int](ints)
		if !IsSorted[int](ints) {
			t.Error("Not sorted")
		}
	}
}

func makeRandomStructs(n int) myStructs {
	rand.Seed(42)
	structs := make([]myStruct, n)
	for i := 0; i < n; i++ {
		structs[i] = myStruct{n: rand.Intn(n)}
	}
	return structs
}

type myStruct struct {
	n int
}
type myStructs []myStruct

func (s myStruct) compareTo(i myStruct) int {
	return s.n - i.n
}

type Int int

func (i Int) compareTo(j Int) int {
	return int(i - j)
}

func BenchmarkSortFuncInts(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		ss := makeRandomStructs(N)
		b.StartTimer()
		ParallelSortFunc(ss)
	}
}
