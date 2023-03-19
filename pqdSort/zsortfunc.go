// Code generated by gen_sort_variants.go; DO NOT EDIT.

// Copyright 2022 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pqdSort

// insertionSortLessFunc sorts data[a:b] using insertion sort.
func insertionSortLessFunc[E any](data []E, a, b int, less func(a, b E) bool) {
	for i := a + 1; i < b; i++ {
		for j := i; j > a && less(data[j], data[j-1]); j-- {
			data[j], data[j-1] = data[j-1], data[j]
		}
	}
}

// siftDownLessFunc implements the heap property on data[lo:hi].
// first is an offset into the array where the root of the heap lies.
func siftDownLessFunc[E any](data []E, lo, hi, first int, less func(a, b E) bool) {
	root := lo
	for {
		child := 2*root + 1
		if child >= hi {
			break
		}
		if child+1 < hi && less(data[first+child], data[first+child+1]) {
			child++
		}
		if !less(data[first+root], data[first+child]) {
			return
		}
		data[first+root], data[first+child] = data[first+child], data[first+root]
		root = child
	}
}

func heapSortLessFunc[E any](data []E, a, b int, less func(a, b E) bool) {
	first := a
	lo := 0
	hi := b - a

	// Build heap with greatest element at top.
	for i := (hi - 1) / 2; i >= 0; i-- {
		siftDownLessFunc(data, i, hi, first, less)
	}

	// Pop elements, largest first, into end of data.
	for i := hi - 1; i >= 0; i-- {
		data[first], data[first+i] = data[first+i], data[first]
		siftDownLessFunc(data, lo, i, first, less)
	}
}

// pdqsortLessFunc sorts data[a:b].
// The algorithm based on pattern-defeating quicksort(pdqsort), but without the optimizations from BlockQuicksort.
// pdqsort paper: https://arxiv.org/pdf/2106.05123.pdf
// C++ implementation: https://github.com/orlp/pdqsort
// Rust implementation: https://docs.rs/pdqsort/latest/pdqsort/
// limit is the number of allowed bad (very unbalanced) pivots before falling back to heapsort.
func pdqsortLessFunc[E any](data []E, a, b, limit int, less func(a, b E) bool) {
	const maxInsertion = 12

	var (
		wasBalanced    = true // whether the last partitioning was reasonably balanced
		wasPartitioned = true // whether the slice was already partitioned
	)

	for {
		length := b - a

		if length <= maxInsertion {
			insertionSortLessFunc(data, a, b, less)
			return
		}

		// Fall back to heapsort if too many bad choices were made.
		if limit == 0 {
			heapSortLessFunc(data, a, b, less)
			return
		}

		// If the last partitioning was imbalanced, we need to breaking patterns.
		if !wasBalanced {
			breakPatternsLessFunc(data, a, b, less)
			limit--
		}

		pivot, hint := choosePivotLessFunc(data, a, b, less)
		if hint == decreasingHint {
			reverseRangeLessFunc(data, a, b, less)
			// The chosen pivot was pivot-a elements after the start of the array.
			// After reversing it is pivot-a elements before the end of the array.
			// The idea came from Rust's implementation.
			pivot = (b - 1) - (pivot - a)
			hint = increasingHint
		}

		// The slice is likely already sorted.
		if wasBalanced && wasPartitioned && hint == increasingHint {
			if partialInsertionSortLessFunc(data, a, b, less) {
				return
			}
		}

		// Probably the slice contains many duplicate elements, partition the slice into
		// elements equal to and elements greater than the pivot.
		if a > 0 && !less(data[a-1], data[pivot]) {
			mid := partitionEqualLessFunc(data, a, b, pivot, less)
			a = mid
			continue
		}

		mid, alreadyPartitioned := partitionLessFunc(data, a, b, pivot, less)
		wasPartitioned = alreadyPartitioned

		leftLen, rightLen := mid-a, b-mid
		balanceThreshold := length / 8
		if leftLen < rightLen {
			wasBalanced = leftLen >= balanceThreshold
			pdqsortLessFunc(data, a, mid, limit, less)
			a = mid + 1
		} else {
			wasBalanced = rightLen >= balanceThreshold
			pdqsortLessFunc(data, mid+1, b, limit, less)
			b = mid
		}
	}
}

// partitionLessFunc does one quicksort partition.
// Let p = data[pivot]
// Moves elements in data[a:b] around, so that data[i]<p and data[j]>=p for i<newpivot and j>newpivot.
// On return, data[newpivot] = p
func partitionLessFunc[E any](data []E, a, b, pivot int, less func(a, b E) bool) (newpivot int, alreadyPartitioned bool) {
	data[a], data[pivot] = data[pivot], data[a]
	i, j := a+1, b-1 // i and j are inclusive of the elements remaining to be partitioned

	for i <= j && less(data[i], data[a]) {
		i++
	}
	for i <= j && !less(data[j], data[a]) {
		j--
	}
	if i > j {
		data[j], data[a] = data[a], data[j]
		return j, true
	}
	data[i], data[j] = data[j], data[i]
	i++
	j--

	for {
		for i <= j && less(data[i], data[a]) {
			i++
		}
		for i <= j && !less(data[j], data[a]) {
			j--
		}
		if i > j {
			break
		}
		data[i], data[j] = data[j], data[i]
		i++
		j--
	}
	data[j], data[a] = data[a], data[j]
	return j, false
}

// partitionEqualLessFunc partitions data[a:b] into elements equal to data[pivot] followed by elements greater than data[pivot].
// It assumed that data[a:b] does not contain elements smaller than the data[pivot].
func partitionEqualLessFunc[E any](data []E, a, b, pivot int, less func(a, b E) bool) (newpivot int) {
	data[a], data[pivot] = data[pivot], data[a]
	i, j := a+1, b-1 // i and j are inclusive of the elements remaining to be partitioned

	for {
		for i <= j && !less(data[a], data[i]) {
			i++
		}
		for i <= j && less(data[a], data[j]) {
			j--
		}
		if i > j {
			break
		}
		data[i], data[j] = data[j], data[i]
		i++
		j--
	}
	return i
}

// partialInsertionSortLessFunc partially sorts a slice, returns true if the slice is sorted at the end.
func partialInsertionSortLessFunc[E any](data []E, a, b int, less func(a, b E) bool) bool {
	const (
		maxSteps         = 5  // maximum number of adjacent out-of-order pairs that will get shifted
		shortestShifting = 50 // don't shift any elements on short arrays
	)
	i := a + 1
	for j := 0; j < maxSteps; j++ {
		for i < b && !less(data[i], data[i-1]) {
			i++
		}

		if i == b {
			return true
		}

		if b-a < shortestShifting {
			return false
		}

		data[i], data[i-1] = data[i-1], data[i]

		// Shift the smaller one to the left.
		if i-a >= 2 {
			for j := i - 1; j >= 1; j-- {
				if !less(data[j], data[j-1]) {
					break
				}
				data[j], data[j-1] = data[j-1], data[j]
			}
		}
		// Shift the greater one to the right.
		if b-i >= 2 {
			for j := i + 1; j < b; j++ {
				if !less(data[j], data[j-1]) {
					break
				}
				data[j], data[j-1] = data[j-1], data[j]
			}
		}
	}
	return false
}

// breakPatternsLessFunc scatters some elements around in an attempt to break some patterns
// that might cause imbalanced partitions in quicksort.
func breakPatternsLessFunc[E any](data []E, a, b int, less func(a, b E) bool) {
	length := b - a
	if length >= 8 {
		random := xorshift(length)
		modulus := nextPowerOfTwo(length)

		for idx := a + (length/4)*2 - 1; idx <= a+(length/4)*2+1; idx++ {
			other := int(uint(random.Next()) & (modulus - 1))
			if other >= length {
				other -= length
			}
			data[idx], data[a+other] = data[a+other], data[idx]
		}
	}
}

// choosePivotLessFunc chooses a pivot in data[a:b].
//
// [0,8): chooses a static pivot.
// [8,shortestNinther): uses the simple median-of-three method.
// [shortestNinther,∞): uses the Tukey ninther method.
func choosePivotLessFunc[E any](data []E, a, b int, less func(a, b E) bool) (pivot int, hint sortedHint) {
	const (
		shortestNinther = 50
		maxSwaps        = 4 * 3
	)

	l := b - a

	var (
		swaps int
		i     = a + l/4*1
		j     = a + l/4*2
		k     = a + l/4*3
	)

	if l >= 8 {
		if l >= shortestNinther {
			// Tukey ninther method, the idea came from Rust's implementation.
			i = medianAdjacentLessFunc(data, i, &swaps, less)
			j = medianAdjacentLessFunc(data, j, &swaps, less)
			k = medianAdjacentLessFunc(data, k, &swaps, less)
		}
		// Find the median among i, j, k and stores it into j.
		j = medianLessFunc(data, i, j, k, &swaps, less)
	}

	switch swaps {
	case 0:
		return j, increasingHint
	case maxSwaps:
		return j, decreasingHint
	default:
		return j, unknownHint
	}
}

// order2LessFunc returns x,y where data[x] <= data[y], where x,y=a,b or x,y=b,a.
func order2LessFunc[E any](data []E, a, b int, swaps *int, less func(a, b E) bool) (int, int) {
	if less(data[b], data[a]) {
		*swaps++
		return b, a
	}
	return a, b
}

// medianLessFunc returns x where data[x] is the median of data[a],data[b],data[c], where x is a, b, or c.
func medianLessFunc[E any](data []E, a, b, c int, swaps *int, less func(a, b E) bool) int {
	a, b = order2LessFunc(data, a, b, swaps, less)
	b, c = order2LessFunc(data, b, c, swaps, less)
	a, b = order2LessFunc(data, a, b, swaps, less)
	return b
}

// medianAdjacentLessFunc finds the median of data[a - 1], data[a], data[a + 1] and stores the index into a.
func medianAdjacentLessFunc[E any](data []E, a int, swaps *int, less func(a, b E) bool) int {
	return medianLessFunc(data, a-1, a, a+1, swaps, less)
}

func reverseRangeLessFunc[E any](data []E, a, b int, less func(a, b E) bool) {
	i := a
	j := b - 1
	for i < j {
		data[i], data[j] = data[j], data[i]
		i++
		j--
	}
}

func swapRangeLessFunc[E any](data []E, a, b, n int, less func(a, b E) bool) {
	for i := 0; i < n; i++ {
		data[a+i], data[b+i] = data[b+i], data[a+i]
	}
}

func stableLessFunc[E any](data []E, n int, less func(a, b E) bool) {
	blockSize := 20 // must be > 0
	a, b := 0, blockSize
	for b <= n {
		insertionSortLessFunc(data, a, b, less)
		a = b
		b += blockSize
	}
	insertionSortLessFunc(data, a, n, less)

	for blockSize < n {
		a, b = 0, 2*blockSize
		for b <= n {
			symMergeLessFunc(data, a, a+blockSize, b, less)
			a = b
			b += 2 * blockSize
		}
		if m := a + blockSize; m < n {
			symMergeLessFunc(data, a, m, n, less)
		}
		blockSize *= 2
	}
}

// symMergeLessFunc merges the two sorted subsequences data[a:m] and data[m:b] using
// the SymMerge algorithm from Pok-Son Kim and Arne Kutzner, "Stable Minimum
// Storage Merging by Symmetric Comparisons", in Susanne Albers and Tomasz
// Radzik, editors, Algorithms - ESA 2004, volume 3221 of Lecture Notes in
// Computer Science, pages 714-723. Springer, 2004.
//
// Let M = m-a and N = b-n. Wolog M < N.
// The recursion depth is bound by ceil(log(N+M)).
// The algorithm needs O(M*log(N/M + 1)) calls to data.Less.
// The algorithm needs O((M+N)*log(M)) calls to data.Swap.
//
// The paper gives O((M+N)*log(M)) as the number of assignments assuming a
// rotation algorithm which uses O(M+N+gcd(M+N)) assignments. The argumentation
// in the paper carries through for Swap operations, especially as the block
// swapping rotate uses only O(M+N) Swaps.
//
// symMerge assumes non-degenerate arguments: a < m && m < b.
// Having the caller check this condition eliminates many leaf recursion calls,
// which improves performance.
func symMergeLessFunc[E any](data []E, a, m, b int, less func(a, b E) bool) {
	// Avoid unnecessary recursions of symMerge
	// by direct insertion of data[a] into data[m:b]
	// if data[a:m] only contains one element.
	if m-a == 1 {
		// Use binary search to find the lowest index i
		// such that data[i] >= data[a] for m <= i < b.
		// Exit the search loop with i == b in case no such index exists.
		i := m
		j := b
		for i < j {
			h := int(uint(i+j) >> 1)
			if less(data[h], data[a]) {
				i = h + 1
			} else {
				j = h
			}
		}
		// Swap values until data[a] reaches the position before i.
		for k := a; k < i-1; k++ {
			data[k], data[k+1] = data[k+1], data[k]
		}
		return
	}

	// Avoid unnecessary recursions of symMerge
	// by direct insertion of data[m] into data[a:m]
	// if data[m:b] only contains one element.
	if b-m == 1 {
		// Use binary search to find the lowest index i
		// such that data[i] > data[m] for a <= i < m.
		// Exit the search loop with i == m in case no such index exists.
		i := a
		j := m
		for i < j {
			h := int(uint(i+j) >> 1)
			if !less(data[m], data[h]) {
				i = h + 1
			} else {
				j = h
			}
		}
		// Swap values until data[m] reaches the position i.
		for k := m; k > i; k-- {
			data[k], data[k-1] = data[k-1], data[k]
		}
		return
	}

	mid := int(uint(a+b) >> 1)
	n := mid + m
	var start, r int
	if m > mid {
		start = n - b
		r = mid
	} else {
		start = a
		r = m
	}
	p := n - 1

	for start < r {
		c := int(uint(start+r) >> 1)
		if !less(data[p-c], data[c]) {
			start = c + 1
		} else {
			r = c
		}
	}

	end := n - start
	if start < m && m < end {
		rotateLessFunc(data, start, m, end, less)
	}
	if a < start && start < mid {
		symMergeLessFunc(data, a, start, mid, less)
	}
	if mid < end && end < b {
		symMergeLessFunc(data, mid, end, b, less)
	}
}

// rotateLessFunc rotates two consecutive blocks u = data[a:m] and v = data[m:b] in data:
// Data of the form 'x u v y' is changed to 'x v u y'.
// rotate performs at most b-a many calls to data.Swap,
// and it assumes non-degenerate arguments: a < m && m < b.
func rotateLessFunc[E any](data []E, a, m, b int, less func(a, b E) bool) {
	i := m - a
	j := b - m

	for i != j {
		if i > j {
			swapRangeLessFunc(data, m-i, m, j, less)
			i -= j
		} else {
			swapRangeLessFunc(data, m-i, m+j-i, i, less)
			j -= i
		}
	}
	// i == j
	swapRangeLessFunc(data, m-i, m, i, less)
}
