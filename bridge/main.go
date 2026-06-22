package main

/*
In some circumstances, you may find yourself wanting to consume values from a
sequence of channels:
<-chan <-chan interface{}
*/
import (
	"context"
	"fmt"
	"math/rand"
)

func bridgeStream(ctx context.Context, streams <-chan <-chan interface{}) <-chan interface{} {
	valStream := make(chan interface{})
	go func() {
		defer close(valStream)
		for {
			var stream <-chan interface{}
			select {
			case <-ctx.Done():
				return
			case maybeStream, ok := <-streams:
				if !ok {
					return
				}
				stream = maybeStream
			}
			for val := range stream {
				select {
				case <-ctx.Done():
					return
				case valStream <- val:
				}
			}
		}
	}()
	return valStream
}
func genStream(ctx context.Context) <-chan <-chan interface{} {
	chanStream := make(chan (<-chan interface{}))
	go func() {
		defer close(chanStream)
		for n := range rand.Intn(10) {
			valStream := make(chan interface{}, 1)
			valStream <- n
			close(valStream)
			select {
			case <-ctx.Done():
				return
			case chanStream <- valStream:
			}
		}
	}()
	return chanStream
}
func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := bridgeStream(ctx, genStream(ctx))
	for val := range stream {
		fmt.Println("Value:", val)
	}
}
