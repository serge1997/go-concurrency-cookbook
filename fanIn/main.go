package main

/*
Fan-in is a term to describe the process of combining
multiple results into one channel.
*/
import (
	"context"
	"fmt"
	"math/rand"
	"sync"
)

func fanInStream(ctx context.Context, streams ...<-chan interface{}) <-chan interface{} {
	fanInStreamResult := make(chan interface{})
	var wg sync.WaitGroup
	wg.Add(len(streams))
	for _, stream := range streams {
		go func(stream <-chan interface{}) {
			defer wg.Done()
			for value := range stream {
				select {
				case <-ctx.Done():
					return
				case fanInStreamResult <- value:
				}
			}
		}(stream)
	}
	go func() {
		wg.Wait()
		close(fanInStreamResult)
	}()
	return fanInStreamResult
}
func intStream(ctx context.Context) <-chan interface{} {
	stream := make(chan interface{})
	go func() {
		defer close(stream)
		for n := range rand.Intn(10) {
			select {
			case <-ctx.Done():
				return
			case stream <- n:
			}
		}
	}()
	return stream
}
func charstream(ctx context.Context) <-chan interface{} {
	stream := make(chan interface{})
	go func() {
		defer close(stream)
		for _, n := range []byte("HELLO WORLD") {
			select {
			case <-ctx.Done():
				return
			case stream <- fmt.Sprintf("%c", n):
			}
		}
	}()
	return stream
}
func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := fanInStream(ctx, intStream(ctx), charstream(ctx))

	for data := range stream {
		fmt.Println(data)
	}
}
