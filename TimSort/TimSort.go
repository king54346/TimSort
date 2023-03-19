package TimSort

import (
	"GoTest/TimSort/fork_join"
	"fmt"
	"golang.org/x/exp/constraints"
	"runtime"
)

//parallelSort

//* @param  work a workspace array (slice)  工作辅助数组
//* @param  workBase origin of usable space in work array 工作辅助数组的扩容参数
//* @param  workLen usable size of work array  工作辅助数组的长度

var (
	min_GALLOP = 7
	min_MERGE  = 32
)

const (
	INITIAL_TMP_STORAGE_LENGTH = 256
	MIN_ARRAY_SORT_GRAN        = 1 << 14
)

func Sort[T constraints.Ordered](a []T) {
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
	comparableTimSort := comparableTimSort[T]{&stack{runBase, runLen, 0}, a, &tmp, 0, len(a)}
	comparableTimSort.sort()
}
func SortRange[T constraints.Ordered](a []T, index1, index2 int) {
	if index1 < index2 {
		Sort(a[index1:index2])
	}
}

type stack struct {
	runBase   []int //runBase[i] + runLen[i] == runBase[i + 1] 每个run首元素的下标
	runLen    []int //run的个数
	stackSize int   //栈的大小
}
type comparableTimSort[T constraints.Ordered] struct {
	ts  *stack
	a   []T
	tmp *[]T
	lo  int
	hi  int
}

/**
对给定的范围进行排序，尽可能使用给定的工作区数组片进行临时存储。
参数: A -要排序的数组
Lo—要排序的第一个元素的索引，包括第一个元素
Hi—要排序的最后一个元素的索引
*/
func (c comparableTimSort[T]) sort() {
	if c.a == nil || c.lo < 0 || c.lo > c.hi || c.hi > len(c.a) {
		panic("assert a != null && lo >= 0 && lo <= hi && hi <= a.length")
	}

	nRemaining := c.hi - c.lo
	if nRemaining < 2 {
		return
	}
	if nRemaining < min_MERGE {
		initRunLen := countRunAndMakeAscending(c.a, c.lo, c.hi)
		binarySort(c.a, c.lo, c.hi, c.lo+initRunLen)
		return
	}
	/**
	1. 把待排序数组分成一个个的run（即单调上升的数组）， 并且run不能太短， 如果run的长度小于minRun这个阀值， 则使用插入排序进行填充
	2. 将上面的一个个run入栈， 当栈顶的run的长度不满足下列约束条件中的任意一个时，则使用归并排序将其中最短的2个run合并成一个新的run，最终栈=1的时候，排序完成。
	　　① runLen[n-2] > runLen[n-1] + runLen[n]
	　　② runLen[n-1] > runLen[n]
	*/
	//计算每个分区的个数
	minRun := minRunLength(nRemaining)
	//循环每个分区
	for {
		runLen := countRunAndMakeAscending(c.a, c.lo, c.hi)
		//如果run的长度小于minRun这个阀值， 则使用插入排序进行填充,填充到min(minRun, nRemaining)
		if runLen < minRun {
			force := minRun
			//最后的个数剩余的个数无法到达最小run
			if nRemaining <= minRun {
				force = nRemaining
			}
			binarySort(c.a, c.lo, c.lo+force, c.lo+runLen)
			runLen = force
		}
		//push到栈中，等待合并
		//mergeCollapse合并
		c.pushRun(c.lo, runLen)
		c.mergeCollapse()

		// Advance to find next run
		c.lo += runLen
		nRemaining -= runLen
		if nRemaining == 0 {
			break
		}
	}
	// Merge all remaining runs to complete sort
	if c.lo != c.hi {
		panic("assert lo == hi")
	}

	c.mergeForceCollapse()
	if c.ts.stackSize != 1 {
		panic("assert stackSize == 1")
	}
}
func (c comparableTimSort[T]) pushRun(runBase int, runLen int) {
	c.ts.runLen[c.ts.stackSize] = runLen
	c.ts.runBase[c.ts.stackSize] = runBase
	c.ts.stackSize++
}

//最后一次合并所有
func (c comparableTimSort[T]) mergeForceCollapse() {
	for c.ts.stackSize > 1 {
		n := c.ts.stackSize - 2
		if n > 0 && c.ts.runLen[n-1] < c.ts.runLen[n+1] {
			n--
		}
		c.mergeAt(n)
	}
}

/**
检查等待合并的run的堆栈，并合并相邻的run，直到不会被合并:
1 runLen[n-2] > runLen[n-1] + runLen[n]
2 runLen[n-1] > runLen[n]
这个方法在每次有新的run push到堆栈时被调用，所以保证在进入该方法时，i < stackSize保持不变
*/
func (c comparableTimSort[T]) mergeCollapse() {
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

/**
合并堆栈索引i和i+1的两次运行。i必须是堆栈上倒数第二个或倒数第二个运行。
换句话说，i必须等于stackSize-2或stackSize-3。
参数: I -堆栈索引的第一个两个运行合并
*/
func (c comparableTimSort[T]) mergeAt(i int) {
	//s.stackSize=3 i=1 和并下标为1和2的
	if c.ts.stackSize < 2 {
		panic("assert stackSize >= 2")
	}
	if i < 0 || i != c.ts.stackSize-2 && i != c.ts.stackSize-3 {
		panic("assert i >= 0 && (i == stackSize - 2 || i == stackSize - 3)")
	}

	base1 := c.ts.runBase[i]
	len1 := c.ts.runLen[i]
	base2 := c.ts.runBase[i+1]
	len2 := c.ts.runLen[i+1]

	if len1 <= 0 || len2 <= 0 || base1+len1 != base2 {
		panic("assert len1 > 0 && len2 > 0 && base1 + len1 == base2")
	}

	/**
	记录合并的run的长度;如果i是最后第三个栈
	i=run2
	[run1,run2,run3,run4]
	run2=run3+run4
	run3=run4
	合并成
	[run1,run2,run3]
	*/
	c.ts.runLen[i] = len1 + len2
	if i == c.ts.stackSize-3 {
		c.ts.runBase[i+1] = c.ts.runBase[i+2]
		c.ts.runLen[i+1] = c.ts.runLen[i+2]
	}
	c.ts.stackSize--
	/**
	找出run2的第一个元素在run1中的位置。
	run1=1367，run2=489 ，从7开始检索4更加小的数，run1中的13将被忽略
	从hint位置比较如果大于key的话从右往左，如果小于key的话从左往右
	*/
	k := gallopRight(c.a[base2], c.a, base1, len1, 0)

	if k < 0 {
		panic("assert k >= 0")
	}
	base1 += k
	len1 -= k
	if len1 == 0 {
		return
	}
	//run1=1367，run2=489 ，从4开始检索7更加大的数，run1中的89将被忽略
	len2 = gallopLeft(c.a[base1+len1-1], c.a,
		base2, len2, len2-1)

	if len2 < 0 {
		panic("assert len2 >= 0")
	}
	if len2 == 0 {
		return
	}

	// 合并剩余的run，使用tmp数组与min(len1, len2)元素
	if len1 <= len2 {
		c.mergeLo(base1, len1, base2, len2)
	} else {
		c.mergeHi(base1, len1, base2, len2)
	}

}

func (c comparableTimSort[T]) mergeLo(base1 int, len1 int, base2 int, len2 int) {
	if len1 <= 0 || len2 <= 0 || base1+len1 != base2 {
		panic("assert len1 > 0 && len2 > 0 && base1 + len1 == base2")
	}

	//67 4  base1=6   base2=4
	tmp := *c.tmp
	if len1 > len(*c.tmp) {
		*c.tmp = append(*c.tmp, make([]T, len1-len(*c.tmp))...)
		tmp = *c.tmp
	}
	cursor1 := 0     // Indexes into tmp array
	cursor2 := base2 // Indexes int a
	dest := base1    // Indexes int a
	//tmp a[67]
	copy(tmp[cursor1:], c.a[base1:base1+len1])
	//	移动第二个run 的第一个元素并处理退化的情况
	c.a[dest] = c.a[cursor2]
	dest++
	cursor2++
	// a[...67489]=>a[...46789]
	// a[...67489]=>a[...48789]
	len2--
	// a[...67489]=>a[...48789]=>a[...46789]
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
			if len1 <= 1 || len2 <= 0 {
				panic("assert len1 > 1 && len2 > 0")
			}
			if len1 <= 1 || len2 <= 0 {
				panic("assert len1 > 1 && len2 > 0")
			}
			if c.a[cursor2] < tmp[cursor1] {
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
			if len1 <= 1 || len2 <= 0 {
				panic("assert len1 > 1 && len2 > 0")
			}
			count1 = gallopRight(c.a[cursor2], tmp, cursor1, len1, 0)
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

			count2 = gallopLeft(tmp[cursor1], c.a, cursor2, len2, 0)
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
	//Write back to field
	if minGallop < 1 {
		min_GALLOP = 1
	} else {
		min_GALLOP = minGallop
	}
	if len1 == 1 {
		if len2 <= 0 {
			panic("assert len2 > 0")
		}

		copy(c.a[dest:], c.a[cursor2:cursor2+len2])
		c.a[dest+len2] = tmp[cursor1] //  Last elt of run 1 to end of merge
	} else if len1 == 0 {
		panic("assert len1 > 0")
	} else {

		if len2 != 0 || len1 <= 1 {
			panic("assert len2 == 0 && len1 > 1")
		}

		copy(c.a[dest:], tmp[cursor1:cursor1+len1])
	}
}

/**
与mergeLo类似，但只有当len1 >= len2时才应该调用该方法;
如果len1 <= len2，则应该调用mergeLo。(如果len1 == len2，可以调用任何一个方法。)
参数:
Base1—第一次运行中要合并的第一个元素的索引
Len1 -第一次被合并运行的长度(必须是> 0)
base2 -第二次运行中要合并的第一个元素的索引(必须是aBase + aLen)
Len2 -要合并的第二次运行的长度(必须是> 0)
*/
func (c comparableTimSort[T]) mergeHi(base1 int, len1 int, base2 int, len2 int) {

	if len1 <= 0 || len2 <= 0 || base1+len1 != base2 {
		panic("assert len1 > 0 && len2 > 0 && base1 + len1 == base2")
	}
	tmp := *c.tmp
	if len2 > len(*c.tmp) {
		*c.tmp = append(*c.tmp, make([]T, len2-len(*c.tmp))...)
		tmp = *c.tmp
	}
	//tmp a[67]
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
	minGallop := min_GALLOP // Use local variable for performance
outer:
	for {
		count1 := 0 // Number of times in a row that first run won
		count2 := 0 // Number of times in a row that second run won
		for {
			if len1 <= 0 || len2 <= 0 {
				panic("assert len1 > 1 && len2 > 0")
			}
			if tmp[cursor2] < c.a[cursor1] {
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

			if len1 <= 0 || len2 <= 1 {
				panic("assert len1 > 0 && len2 > 1")
			}

			count1 = len1 - gallopRight(tmp[cursor2], c.a, base1, len1, len1-1)
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

			count2 = len2 - gallopLeft(c.a[cursor1], tmp, 0, len2, len2-1)
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
		minGallop += 2 // Penalize for leaving gallop mode
	}
	//Write back to field
	if minGallop < 1 {
		min_GALLOP = 1
	} else {
		min_GALLOP = minGallop
	}
	if len2 == 1 {
		if len1 <= 0 {
			panic("assert len1 > 0")
		}
		dest -= len1
		cursor1 -= len1
		copy(c.a[dest+1:], c.a[cursor1+1:cursor1+1+len1])
		c.a[dest] = tmp[cursor2] // Move first elt of run2 to front of merge
	} else if len2 == 0 {
		panic("assert len2 > 0")
	} else {
		if len1 != 0 || len2 <= 0 {
			panic("assert len1 == 0 && len2 > 0")
		}
		copy(c.a[dest-(len2-1):], tmp[:len2])
	}
}

func gallopLeft[T constraints.Ordered](key T, a []T, base int, len int, hint int) int {
	if len <= 0 || hint < 0 || hint >= len {
		panic("assert len > 0 && hint >= 0 && hint < len")
	}
	lastOfs := 0
	ofs := 1
	if key > a[base+hint] {
		//从左往右 until a[base+hint+lastOfs] < key <= a[base+hint+ofs]
		maxOfs := len - hint
		for ofs < maxOfs && key > a[base+hint+ofs] {
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
	} else { // key <= a[base + hint]
		// 从右往左 until a[base+hint-ofs] < key <= a[base+hint-lastOfs]
		maxOfs := hint + 1
		for ofs < maxOfs && key <= a[base+hint-ofs] {
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
		lastOfs, ofs = hint-ofs, hint-lastOfs
	}

	if -1 > lastOfs || lastOfs >= ofs || ofs > len {
		panic("assert -1 <= lastOfs && lastOfs < ofs && ofs <= len")
	}
	/*
	 *  a[base+lastOfs] < key <= a[base+ofs] Do a binary
	 * search, with invariant a[base + lastOfs - 1] < key <= a[base + ofs].
	 */
	lastOfs++
	for lastOfs < ofs {
		m := lastOfs + ((ofs - lastOfs) >> 1)
		if key > a[base+m] {
			lastOfs = m + 1 // a[base + m] < key
		} else {
			ofs = m // key <= a[base + m]
		}
	}
	//a[base + ofs - 1] < key <= a[base + ofs]
	if lastOfs != ofs {
		panic("assert lastOfs == ofs")
	}

	return ofs
}

/**
与gallopLeft一样，不同之处在于，如果范围包含一个等于key的元素，那么gallopRight将返回最右边相等元素之后的索引。
参数:
Key -要搜索其插入点的键
A -要搜索的数组
Base—范围内第一个元素的索引
Len—范围的长度;必须是> 0
hint—开始搜索的索引，0 <= hint < n。
hint越靠近结果，这个方法运行得越快。
返回: int k, 0 <= k <= n使a[b + k - 1] <= key < a[b + k]
*/
func gallopRight[T constraints.Ordered](key T, a []T, base int, len int, hint int) int {

	if len <= 0 || hint < 0 || hint >= len {
		panic("assert len > 0 && hint >= 0 && hint < len")
	}
	ofs := 1
	lastOfs := 0
	if key < a[base+hint] {
		//从右往左检索 直到a[b+hint - ofs] <= key < a[b+hint - lastOfs]
		maxOfs := hint + 1
		for ofs < maxOfs && key < a[base+hint-ofs] {
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
		lastOfs, ofs = hint-ofs, hint-lastOfs

	} else { // a[b + hint] <= key
		maxOfs := len - hint
		// 从左到右 until a[b+hint + lastOfs] <= key < a[b+hint + ofs]
		for ofs < maxOfs && key >= a[base+hint+ofs] {
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
	//现在a[b + lastOfs] <= key < a[b + ofs]
	//通过二分搜索找到a[b + lastOfs - 1] <= key < a[b + ofs]
	for lastOfs < ofs {
		m := lastOfs + ((ofs - lastOfs) >> 1)

		if key < a[base+m] {
			ofs = m // key < a[b + m]
		} else {
			lastOfs = m + 1 // a[b + m] <= key
		}
	}
	// so a[b + ofs - 1] <= key < a[b + ofs]
	if lastOfs != ofs {
		panic("assert lastOfs == ofs")
	}

	return ofs
}

/**
使用二进制插入排序对指定数组的指定部分进行排序。这是对少量元素进行排序的最佳方法。它需要O(n log n)的比较，但是O(n^2)的数据移动(最坏情况)。
如果指定范围的初始部分已经排好序，这个方法就可以利用它:该方法假设从index lo, inclusive, start, exclusive开始的元素已经排好序。
参数:
  A -要对一个范围进行排序的数组
  Lo—要排序的范围内第一个元素的下标
  Hi -要排序的范围内最后一个元素之后的索引
  Start -范围内第一个未知排序元素的索引(lo <= Start <= hi)
*/
func binarySort[T constraints.Ordered](src []T, low int, high int, start int) {

	if low > start || start > high {
		panic("assert low <= start && start <= high")
	}
	if start == low {
		start++
	}
	for ; start < high; start++ {
		left := low
		right := start
		pivot := src[start]
		for left < right {
			mid := (left + right) >> 1
			// src[start] >= all in [lo, left).
			// src[start] <  all in [right, start)
			if src[mid] > pivot {
				right = mid
			} else {
				left = mid + 1
			}
		}
		//判断 left==right
		//将前面所有大于当前待插入记录的记录后移
		n := start - left //要移动的元素的数量
		//2或者1 不需要整体移动
		switch n {
		case 2:
			src[left+1], src[left+2] = src[left], src[left+1]
		case 1:
			src[left+1] = src[left]
		default:
			copy(src[left+1:], src[left:left+n]) //向后移动n
		}
		//将待插入记录回填到正确位置.
		src[left] = pivot
	}
}

/**单调序列
A -单调序列的数组
low -第一个元素的索引
hight -最后一个元素之后的索引
*/
func countRunAndMakeAscending[T constraints.Ordered](a []T, low int, high int) int {
	runHi := low + 1
	if runHi == high {
		return 1
	}

	if a[runHi] < a[low] { // Descending
		runHi++
		for runHi < high && a[runHi] < a[runHi-1] {
			runHi++
		}
		//反转切片
		reverseRange(a, low, runHi)
	} else { // Ascending
		runHi++
		for runHi < high && a[runHi] >= a[runHi-1] {
			runHi++
		}
	}

	return runHi - low
}

/**
A -要反转一个范围的数组
Lo—要反转的范围内第一个元素的索引
Hi -要反转的范围内最后一个元素之后的索引
*/
func reverseRange[T constraints.Ordered](a []T, low int, high int) {
	high--
	for low < high {
		a[low], a[high] = a[high], a[low]
		low++
		high--
	}
}

//是否是单调序列
func IsSorted[T constraints.Ordered](a []T) bool {
	n := len(a)
	for i := n - 1; i > 0; i-- {
		if a[i] < a[i-1] {
			return false
		}
	}
	return true
}

/**
返回指定长度的数组的最小可接受运行长度。如果小于MIN_MERGE使用binarySort
粗略地说，计算是这样的:
如果n < min_MERGE，返回n。
如果n是2的精确幂，返回MIN_MERGE/2。
否则返回一个int k, min_MERGE/2 <= k <= min_MERGE，这样n/k接近但严格小于2的精确幂。
有关基本原理，请参阅listsort.txt。
参数: N -要排序的数组的长度 返回: 要合并的最小运行的长度
*/
func minRunLength(n int) int {
	r := 0 // Becomes 1 if any 1 bits are shifted off
	for n >= min_MERGE {
		r |= n & 1
		n >>= 1
	}
	return n + r
}

type SortParallel[T constraints.Ordered] struct {
	startIndex int
	endIndex   int
	array      []T
	gran       int //分割的最小粒度
	fork_join.ForkJoinTask
	pool *fork_join.ForkJoinPool
}

func (s *SortParallel[T]) Compute() interface{} {

	defer func() {
		if p := recover(); p != nil {
			fmt.Printf("here is err %#v\n", p)
		}
	}()
	var array []T
	if s.endIndex-s.startIndex < s.gran {
		array = s.array[s.startIndex:s.endIndex]
		Sort(array)
	} else {
		mid := (s.endIndex - s.startIndex) >> 1
		sTask1 := &SortParallel[T]{startIndex: s.startIndex, endIndex: s.startIndex + mid, array: s.array, gran: s.gran, pool: s.pool}
		sTask2 := &SortParallel[T]{startIndex: s.startIndex + mid, endIndex: s.endIndex, array: s.array, gran: s.gran, pool: s.pool}
		sTask1.Build(s.pool).Run(sTask1)
		sTask2.Build(s.pool).Run(sTask2)
		ok1, r1 := sTask1.Join()
		ok2, r2 := sTask2.Join()
		if ok1 && ok2 {
			array = append(r1.([]T), r2.([]T)...)
			Sort(array)
		}
	}
	return array
}

func ParallelSort[T constraints.Ordered](a []T) []T {

	if len(a) < MIN_ARRAY_SORT_GRAN || runtime.NumCPU() == 1 {
		Sort(a)
		return a
	}
	gran := len(a) >> 3
	taskPool := fork_join.NewForkJoinPool(int32(runtime.NumCPU()))
	s := &SortParallel[T]{startIndex: 0, endIndex: len(a), array: a, gran: gran, pool: taskPool}
	v2 := s.Compute()
	return v2.([]T)
}
