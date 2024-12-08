package raft

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

type SaftChan struct {
	mu    sync.Mutex
	c     chan interface{}
	close bool
}

func (sc *SaftChan) insert(in interface{}) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if !sc.close {
		sc.c <- in
	}
}

func (sc *SaftChan) Close() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if !sc.close {
		sc.close = true
		close(sc.c)
	}
}

func NewSc(c chan interface{}) *SaftChan {
	return &SaftChan{
		mu:    sync.Mutex{},
		c:     c,
		close: false,
	}
}

func (rf *Raft) DoFunc(fn func(int, interface{}, interface{}) bool, arg, reply interface{}, duration time.Duration) (result chan interface{}) {
	result = make(chan interface{})
	wg := sync.WaitGroup{}
	cresult := int32(0)
	sc := NewSc(result)

	if rf == nil {
		sc.insert(fmt.Errorf("rf is null"))
		return
	}

	go func() {
		time.Sleep(duration)
		sc.Close()
	}()

	for i := range rf.peers {
		wg.Add(1)
		go func(x int) {
			defer func() {
				wg.Done()
			}()

			newReply := reflect.New(reflect.TypeOf(reply).Elem()).Interface()
			success := fn(x, arg, newReply)
			if atomic.LoadInt32(&cresult) != 0 {
				return
			}
			if !success {
				sc.insert(fmt.Errorf("%d do fucn %s false", x, reflect.TypeOf(fn).Name()))
			} else {
				sc.insert(newReply)
			}
		}(i)
	}

	go func() {
		wg.Wait()
		sc.Close()
	}()

	return
}
