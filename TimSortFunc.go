package TimSort2

import (
	"GoTest/TimSort/fork_join"
	"fmt"
	"runtime"
)

//  compareto 用于比较两个元素的大小 1 代表大于 0 代表等于 -1 代表小于
type Comparable[T any] interface {
	compareTo(o T) int
}
type comparableTimSortFunc[T Comparable[T]] struct {
	ts  *stack
	a   []T
	tmp *[]T
	lo  int
	hi  int
}

func SortFunc[T Comparable[T]](a []T) {
	stackLen := func(s int) (tmp int) {
		switch {
		case s < 120:
			tmp = 5
		case s < 1542:
			tmp = 10
		case s < 119151:
			tmp = 24
		default:
			tmp = 49

		}
		return
	}
	runBase := make([]int, stackLen(len(a)))
	runLen := make([]int, stackLen(len(a)))
	tlen := func() int {
		if len(a) < 2*INITIAL_TMP_STORAGE_LENGTH {
			return len(a) >> 1
		} else {
			return INITIAL_TMP_STORAGE_LENGTH
		}
	}
	tmp := make([]T, tlen())
	comparableTimSort := comparableTimSortFunc[T]{&stack{runBase, runLen, 0}, a, &tmp, 0, len(a)}
	comparableTimSort.sort()
}
func (c comparableTimSortFunc[T]) sort() {
	nRemaining := c.hi - c.lo
	if nRemaining < 2 {
		return
	}
	if nRemaining < min_MERGE {
		initRunLen := countRunAndMakeAscendingFunc(c.a, c.lo, c.hi)
		binarySortFunc(c.a, c.lo, c.hi, c.lo+initRunLen)
		return
	}
	minRun := minRunLength(nRemaining)
	for {
		runLen := countRunAndMakeAscendingFunc(c.a, c.lo, c.hi)
		if runLen < minRun {
			force := minRun
			if nRemaining <= minRun {
				force = nRemaining
			}
			binarySortFunc(c.a, c.lo, c.lo+force, c.lo+runLen)
			runLen = force
		}
		c.pushRun(c.lo, runLen)
		c.mergeCollapse()
		c.lo += runLen
		nRemaining -= runLen
		if nRemaining == 0 {
			break
		}
	}
	c.mergeForceCollapse()
}
func (c comparableTimSortFunc[T]) pushRun(runBase int, runLen int) {
	c.ts.runLen[c.ts.stackSize] = runLen
	c.ts.runBase[c.ts.stackSize] = runBase
	c.ts.stackSize++
}
func (c comparableTimSortFunc[T]) mergeForceCollapse() {
	for c.ts.stackSize > 1 {
		n := c.ts.stackSize - 2
		if n > 0 && c.ts.runLen[n-1] < c.ts.runLen[n+1] {
			n--
		}
		c.mergeAt(n)
	}
}
func (c comparableTimSortFunc[T]) mergeCollapse() {
	for c.ts.stackSize > 1 {
		n := c.ts.stackSize - 2
		//s.stackSize=3 n=1 run[0]>run[1]+run[2]
		if n > 0 && c.ts.runLen[n-1] <= c.ts.runLen[n]+c.ts.runLen[n+1] {
			if c.ts.runLen[n-1] < c.ts.runLen[n+1] {
				n--
			}
			c.mergeAt(n)
		} else if c.ts.runLen[n] <= c.ts.runLen[n+1] {
			c.mergeAt(n)
		} else {
			break
		}
	}
}
func (c comparableTimSortFunc[T]) mergeAt(i int) {
	base1 := c.ts.runBase[i]
	len1 := c.ts.runLen[i]
	base2 := c.ts.runBase[i+1]
	len2 := c.ts.runLen[i+1]
	c.ts.runLen[i] = len1 + len2
	if i == c.ts.stackSize-3 {
		c.ts.runBase[i+1] = c.ts.runBase[i+2]
		c.ts.runLen[i+1] = c.ts.runLen[i+2]
	}
	c.ts.stackSize--
	k := gallopRightFunc(c.a[base2], c.a, base1, len1, 0)
	base1 += k
	len1 -= k
	if len1 == 0 {
		return
	}
	len2 = gallopLeftFunc(c.a[base1+len1-1], c.a,
		base2, len2, len2-1)
	if len2 == 0 {
		return
	}
	if len1 <= len2 {
		c.mergeLo(base1, len1, base2, len2)
	} else {
		c.mergeHi(base1, len1, base2, len2)
	}

}
func (c comparableTimSortFunc[T]) mergeLo(base1 int, len1 int, base2 int, len2 int) {
	tmp := *c.tmp
	if len1 > len(*c.tmp) {
		*c.tmp = append(*c.tmp, make([]T, len1-len(*c.tmp))...)
		tmp = *c.tmp
	}
	cursor1 := 0     // Indexes into tmp array
	cursor2 := base2 // Indexes int a
	dest := base1    // Indexes int a
	copy(tmp[cursor1:], c.a[base1:base1+len1])
	c.a[dest] = c.a[cursor2]
	dest++
	cursor2++
	len2--
	if len2 == 0 {
		copy(c.a[dest:], tmp[cursor1:cursor1+len1])
		return
	}

	if len1 == 1 {
		copy(c.a[dest:], c.a[cursor2:cursor2+len2])
		c.a[dest+len2] = tmp[cursor1] // Last elt of run 1 to end of merge
		return
	}
	minGallop := min_GALLOP // Use local variable for performance
outer:
	for {
		count1 := 0 // Number of times in a row that first run won
		count2 := 0 // Number of times in a row that second run won
		for {
			if c.a[cursor2].compareTo(tmp[cursor1]) < 0 {
				c.a[dest] = c.a[cursor2]
				dest++
				cursor2++
				count2++
				count1 = 0
				len2--
				if len2 == 0 {
					break outer
				}

			} else {
				c.a[dest] = tmp[cursor1]
				dest++
				cursor1++
				count1++
				count2 = 0
				len1--
				if len1 == 1 {
					break outer
				}
				if minGallop < (count1 | count2) {
					break
				}
			}
		}
		for {
			count1 = gallopRightFunc(c.a[cursor2], tmp, cursor1, len1, 0)
			if count1 != 0 {
				copy(c.a[dest:], tmp[cursor1:cursor1+count1])
				dest += count1
				cursor1 += count1
				len1 -= count1
				if len1 <= 1 { // len1 == 1 || len1 == 0
					break outer
				}
			}
			c.a[dest] = c.a[cursor2]
			dest++
			cursor2++
			len2--
			if len2 == 0 {
				break outer
			}

			count2 = gallopLeftFunc(tmp[cursor1], c.a, cursor2, len2, 0)
			if count2 != 0 {
				copy(c.a[dest:], c.a[cursor2:cursor2+count2])
				dest += count2
				cursor2 += count2
				len2 -= count2
				if len2 == 0 {
					break outer
				}
			}
			c.a[dest] = tmp[cursor1]
			cursor1++
			dest++
			len1--
			if len1 == 1 {
				break outer
			}
			minGallop--

			if !(count1 >= min_GALLOP || count2 >= min_GALLOP) {
				break
			}
		}
		if minGallop < 0 {
			minGallop = 0
		}
		minGallop += 2 // Penalize for leaving gallop mode
	}
	if minGallop < 1 {
		min_GALLOP = 1
	} else {
		min_GALLOP = minGallop
	}
	if len1 == 1 {
		copy(c.a[dest:], c.a[cursor2:cursor2+len2])
		c.a[dest+len2] = tmp[cursor1] //  Last elt of run 1 to end of merge
	} else if len1 == 0 {
		//todo error
	} else {
		copy(c.a[dest:], tmp[cursor1:cursor1+len1])
	}
}
func (c comparableTimSortFunc[T]) mergeHi(base1 int, len1 int, base2 int, len2 int) {
	tmp := *c.tmp
	if len2 > len(*c.tmp) {
		*c.tmp = append(*c.tmp, make([]T, len2-len(*c.tmp))...)
		tmp = *c.tmp
	}
	copy(tmp[:], c.a[base2:base2+len2])
	cursor1 := base1 + len1 - 1 // Indexes into a
	cursor2 := len2 - 1         // Indexes into tmp array
	dest := base2 + len2 - 1    // Indexes into a
	c.a[dest] = c.a[cursor1]
	dest--
	cursor1--
	len1--
	if len1 == 0 {
		copy(c.a[dest-(len2-1):], tmp[:len2])
		return
	}
	if len2 == 1 {
		dest -= len1
		cursor1 -= len1
		copy(c.a[dest+1:], c.a[cursor1+1:cursor1+1+len1])
		c.a[dest] = tmp[cursor2]
		return
	}
	minGallop := min_GALLOP
outer:
	for {
		count1 := 0 // Number of times in a row that first run won
		count2 := 0 // Number of times in a row that second run won
		for {
			if tmp[cursor2].compareTo(c.a[cursor1]) < 0 {
				c.a[dest] = c.a[cursor1]
				dest--
				cursor1--
				count1++
				count2 = 0
				len1--
				if len1 == 0 {
					break outer
				}

			} else {
				c.a[dest] = tmp[cursor2]
				dest--
				cursor2--
				count2++
				count1 = 0
				len2--
				if len2 == 1 {
					break outer
				}
				if minGallop < (count1 | count2) {
					break
				}
			}
		}
		for {
			count1 = len1 - gallopRightFunc(tmp[cursor2], c.a, base1, len1, len1-1)
			if count1 != 0 {
				dest -= count1
				cursor1 -= count1
				len1 -= count1
				copy(c.a[dest+1:], c.a[cursor1+1:cursor1+1+count1])
				if len1 == 0 {
					break outer
				}
			}
			c.a[dest] = tmp[cursor2]
			dest--
			cursor2--
			len2--
			if len2 == 1 {
				break outer
			}

			count2 = len2 - gallopLeftFunc(c.a[cursor1], tmp, 0, len2, len2-1)
			if count2 != 0 {
				dest -= count2
				cursor2 -= count2
				len2 -= count2
				copy(c.a[dest+1:], tmp[cursor2+1:cursor2+1+count2])
				if len2 <= 1 {
					break outer // len2 == 1 || len2 == 0
				}
			}
			c.a[dest] = c.a[cursor1]
			cursor1--
			dest--
			len1--
			if len1 == 0 {
				break outer
			}
			minGallop--

			if !(count1 >= min_GALLOP || count2 >= min_GALLOP) {
				break
			}
		}
		if minGallop < 0 {
			minGallop = 0
		}
		minGallop += 2
	}
	if minGallop < 1 {
		min_GALLOP = 1
	} else {
		min_GALLOP = minGallop
	}
	if len2 == 1 {
		dest -= len1
		cursor1 -= len1
		copy(c.a[dest+1:], c.a[cursor1+1:cursor1+1+len1])
		c.a[dest] = tmp[cursor2]
	} else if len2 == 0 {
		//todo error
	} else {
		copy(c.a[dest-(len2-1):], tmp[:len2])
	}
}
func gallopRightFunc[T Comparable[T]](key T, a []T, base int, len int, hint int) int {
	ofs := 1
	lastOfs := 0
	if key.compareTo(a[base+hint]) < 0 {
		maxOfs := hint + 1
		for ofs < maxOfs && key.compareTo(a[base+hint-ofs]) < 0 {
			lastOfs = ofs
			ofs = (ofs << 1) + 1
			if ofs <= 0 { // int overflow
				ofs = maxOfs
			}
		}
		if ofs > maxOfs {
			ofs = maxOfs
		}
		lastOfs, ofs = hint-ofs, hint-lastOfs

	} else {
		maxOfs := len - hint
		for ofs < maxOfs && key.compareTo(a[base+hint+ofs]) >= 0 {
			lastOfs = ofs
			ofs = (ofs << 1) + 1
			if ofs <= 0 { // int overflow
				ofs = maxOfs
			}
		}
		if ofs > maxOfs {
			ofs = maxOfs
		}
		// 相对与base进行偏移
		lastOfs += hint
		ofs += hint
	}
	lastOfs++
	for lastOfs < ofs {
		m := lastOfs + ((ofs - lastOfs) >> 1)

		if key.compareTo(a[base+m]) < 0 {
			ofs = m // key < a[b + m]
		} else {
			lastOfs = m + 1 // a[b + m] <= key
		}
	}
	return ofs
}
func gallopLeftFunc[T Comparable[T]](key T, a []T, base int, len int, hint int) int {
	lastOfs := 0
	ofs := 1
	if key.compareTo(a[base+hint]) > 0 {
		maxOfs := len - hint
		for ofs < maxOfs && key.compareTo(a[base+hint+ofs]) > 0 {
			lastOfs = ofs
			ofs = (ofs << 1) + 1
			if ofs <= 0 { // int overflow
				ofs = maxOfs
			}
		}
		if ofs > maxOfs {
			ofs = maxOfs
		}
		lastOfs += hint
		ofs += hint
	} else {
		maxOfs := hint + 1
		for ofs < maxOfs && key.compareTo(a[base+hint-ofs]) <= 0 {
			lastOfs = ofs
			ofs = (ofs << 1) + 1
			if ofs <= 0 { // int overflow
				ofs = maxOfs
			}
		}
		if ofs > maxOfs {
			ofs = maxOfs
		}
		lastOfs, ofs = hint-ofs, hint-lastOfs
	}
	lastOfs++
	for lastOfs < ofs {
		m := lastOfs + ((ofs - lastOfs) >> 1)
		if key.compareTo(a[base+m]) > 0 {
			lastOfs = m + 1 // a[base + m] < key
		} else {
			ofs = m // key <= a[base + m]
		}
	}
	return ofs
}
func binarySortFunc[T Comparable[T]](src []T, low int, high int, start int) {
	if start == low {
		start++
	}
	for ; start < high; start++ {
		left := low
		right := start
		pivot := src[start]
		for left < right {
			mid := (left + right) >> 1
			if src[mid].compareTo(pivot) > 0 {
				right = mid
			} else {
				left = mid + 1
			}
		}
		n := start - left //要移动的元素的数量
		switch n {
		case 2:
			src[left+1], src[left+2] = src[left], src[left+1]
		case 1:
			src[left+1] = src[left]
		default:
			copy(src[left+1:], src[left:left+n]) //向后移动n
		}
		src[left] = pivot
	}
}
func countRunAndMakeAscendingFunc[T Comparable[T]](a []T, low int, high int) int {
	runHi := low + 1
	if runHi == high {
		return 1
	}

	if a[runHi].compareTo(a[low]) < 0 { // Descending
		runHi++
		for runHi < high && a[runHi].compareTo(a[runHi-1]) < 0 {
			runHi++
		}
		reverseRangeFunc(a, low, runHi)
	} else {
		runHi++
		for runHi < high && a[runHi].compareTo(a[runHi-1]) >= 0 {
			runHi++
		}
	}

	return runHi - low
}
func reverseRangeFunc[T Comparable[T]](a []T, low int, high int) {
	high--
	for low < high {
		a[low], a[high] = a[high], a[low]
		low++
		high--
	}
}

//是否是单调序列
func IsSortedFunc[T Comparable[T]](a []T) bool {
	n := len(a)
	for i := n - 1; i > 0; i-- {
		if a[i].compareTo(a[i-1]) < 0 {
			return false
		}
	}
	return true
}

type SortFuncParallel[T Comparable[T]] struct {
	startIndex int
	endIndex   int
	array      []T
	gran       int //分割的最小粒度
	fork_join.ForkJoinTask
	pool *fork_join.ForkJoinPool
}

func (s *SortFuncParallel[T]) Compute() interface{} {
	//var taskPool=fork_join.NewForkJoinPool(int32(runtime.NumCPU()))
	s.TaskPool = fork_join.NewForkJoinPool(int32(runtime.NumCPU()))
	defer func() {
		if p := recover(); p != nil {
			fmt.Printf("here is err %#v\n", p)
		}
	}()
	var array []T
	if s.endIndex-s.startIndex < s.gran {
		array = s.array[s.startIndex:s.endIndex]
		SortFunc(array)
	} else {
		h := (s.endIndex - s.startIndex) >> 1
		q := h >> 1
		u := h + q
		sTask1 := &SortFuncParallel[T]{startIndex: s.startIndex, endIndex: s.startIndex + h, array: s.array, gran: s.gran, pool: s.pool}
		sTask2 := &SortFuncParallel[T]{startIndex: s.startIndex + h, endIndex: s.startIndex + u, array: s.array, gran: s.gran, pool: s.pool}
		sTask3 := &SortFuncParallel[T]{startIndex: s.startIndex + u, endIndex: s.endIndex, array: s.array, gran: s.gran, pool: s.pool}
		sTask1.Build(s.TaskPool).Run(sTask1)
		sTask2.Build(s.TaskPool).Run(sTask2)
		sTask3.Build(s.TaskPool).Run(sTask3)
		ok1, r1 := sTask1.Join()
		ok2, r2 := sTask2.Join()
		ok3, r3 := sTask3.Join()
		if ok1 && ok2 && ok3 {
			array = append(r1.([]T), r2.([]T)...)
			array = append(array, r3.([]T)...)
			SortFunc(array)
		}
	}
	return array
}

func ParallelSortFunc[T Comparable[T]](a []T) []T {
	if len(a) < MIN_ARRAY_SORT_GRAN || runtime.NumCPU() == 1 {
		SortFunc(a)
		return a
	}
	gran := len(a) >> 3
	taskPool := fork_join.NewForkJoinPool(int32(runtime.NumCPU()))
	s := &SortFuncParallel[T]{startIndex: 0, endIndex: len(a), array: a, gran: gran, pool: taskPool}
	v2 := s.Compute()
	return v2.([]T)
}
