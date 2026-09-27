//go:build unix

package gda

import (
	"context"
	"fmt"
	"sync"
)

// workerID returns the ID of worker i of a run.
func (b *backup) workerID(i int) string {
	return workerID(b.opt, i)
}

// workerID returns the ID of worker i of a run with options opt.
func workerID(opt Options, i int) string {
	if opt.Workers == 1 {
		return opt.Worker
	}
	return fmt.Sprintf("%s-%02d", opt.Worker, i+1)
}

// summarizeAll computes the summaries of the whole source, scanning
// subdirectories in parallel with as many workers as the run has.
func (b *backup) summarizeAll() {
	var sem chan struct{}
	if b.opt.Workers > 1 {
		sem = make(chan struct{}, b.opt.Workers-1)
	}
	b.summarize("", sem)
}

// processAll backs up every directory with its own index, starting at
// the root. Each directory is processed by exactly one worker, so its
// changeset and index have a single writer.
func (b *backup) processAll(ctx context.Context) {
	b.processItems(ctx, []childDir{{}})
}

// processItems backs up the directories in items, and the directories
// with their own index below those which aren't shallow, with the run's
// workers taking directories from a shared queue.
func (b *backup) processItems(ctx context.Context, items []childDir) {
	q := newWorkQueue()
	for _, item := range items {
		q.push(item)
	}
	var wg sync.WaitGroup
	for i := range b.opt.Workers {
		w := b.workerID(i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				item, ok := q.pop()
				if !ok {
					return
				}
				children := b.processDir(ctx, w, item.rel, item.key, b.isRollupRoot(item.rel))
				if !item.shallow {
					for _, child := range children {
						if child.unrolled && b.dirty != nil {
							// A change run only has totals for this
							// subtree, so it scans it before indexing it.
							b.summarize(child.rel, nil)
							b.markNew(child.rel)
						}
						if b.wanted(child.rel) {
							q.push(child)
						}
					}
				}
				q.done()
			}
		}()
	}
	wg.Wait()
}

// workQueue is a queue of directories to process which knows when all
// the work is done, including the work that processing adds.
type workQueue struct {
	mu      sync.Mutex
	cond    *sync.Cond
	items   []childDir
	pending int // items pushed but not done
}

func newWorkQueue() *workQueue {
	q := &workQueue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// push adds item to the queue.
func (q *workQueue) push(item childDir) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.items = append(q.items, item)
	q.pending++
	q.cond.Signal()
}

// pop takes an item from the queue, waiting while other items are in
// progress. ok is false once everything is done.
func (q *workQueue) pop() (item childDir, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) == 0 && q.pending > 0 {
		q.cond.Wait()
	}
	if len(q.items) == 0 {
		return item, false
	}
	item = q.items[len(q.items)-1]
	q.items = q.items[:len(q.items)-1]
	return item, true
}

// done marks an item popped from the queue as finished. Items it added
// must be pushed first.
func (q *workQueue) done() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pending--
	if q.pending == 0 {
		q.cond.Broadcast()
	}
}
