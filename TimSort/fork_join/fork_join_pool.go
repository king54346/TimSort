package fork_join

import (
	"context"
	"fmt"
	"sync"
)

type ForkJoinPool struct {
	cap         int32
	taskQueue   *TaskQueue
	wp          *Pool // worker pool
	lock        sync.Mutex
	signal      *sync.Cond // 用于通知 worker pool 中的 worker 有新的任务到来
	goroutineID int32
	ctx         context.Context
	cancel      context.CancelFunc
	err         interface{}
}

func NewForkJoinPool(workerCap int32) *ForkJoinPool {
	ctx, cancel := context.WithCancel(context.Background())
	fp := &ForkJoinPool{
		cap:       workerCap,
		taskQueue: NewTaskQueue(workerCap),
		ctx:       ctx,
		cancel:    cancel,
	}
	fp.wp = newPool(cancel)
	fp.signal = sync.NewCond(new(sync.Mutex))
	fp.run(ctx)
	return fp
}

func (fp *ForkJoinPool) SetPanicHandler(panicHandler func(interface{})) {
	fp.wp.panicHandler = panicHandler
}

func (fp *ForkJoinPool) pushTask(t Task, f *ForkJoinTask) {
	fp.taskQueue.enqueue(t, f)
}

// 每个 worker 轮询自己对应的 Task 队列进行获取任务
func (fp *ForkJoinPool) run(ctx context.Context) {
	go func() {
		wId := int32(0)
		for {
			select {
			case <-ctx.Done():
				fmt.Printf("here is err")
				fp.err = fp.wp.err
				return
			default:
				hasTask, job, ft := fp.taskQueue.dequeueByTali(wId)
				if hasTask {
					fp.wp.Submit(ctx, &struct {
						T Task
						F *ForkJoinTask
						C context.Context
					}{T: job, F: ft, C: ctx})
				}
				wId = (wId + 1) % fp.cap
			}
		}
	}()
}
