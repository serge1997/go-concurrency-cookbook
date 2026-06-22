package main

/*
Confinement in Go is a concurrency pattern that ensures a piece of data is only ever
accessible by a single goroutine at a time. Here, streamOwner is the sole owner of
the channel — only it can write and close. The read-only channel (<-chan interface{})
exposed to the caller makes this guarantee compile-time enforced.
*/

import (
	"context"
	"fmt"
	"math/rand"
)

func streamOwner(ctx context.Context) <-chan interface{} {
	stream := make(chan interface{})
	go func() {
		defer close(stream)
		for n := range rand.Intn(10) {
			select {
			case <-ctx.Done():
				return
			case stream <- fmt.Sprintf("produce: %v\n", n):
			}
		}
	}()
	return stream
}
func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := streamOwner(ctx)
	for data := range stream {
		fmt.Println(data)
	}
}
