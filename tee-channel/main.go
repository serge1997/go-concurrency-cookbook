package main

/*
Sometimes you may want to split values coming in from a channel so that you can
send them off into two separate areas of your codebase. Imagine a channel of user
commands: you might want to take in a stream of user commands on a channel, send
them to something that executes them, and also send them to something that logs the
commands for later auditing.
*/
import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
)

func teeStream(ctx context.Context, mainStream <-chan interface{}) (<-chan interface{}, <-chan interface{}) {
	workerStream := make(chan interface{})
	logStream := make(chan interface{})
	go func() {
		defer close(workerStream)
		defer close(logStream)
		for data := range mainStream {
			workerStream := workerStream
			logStream := logStream
			for range 2 {
				select {
				case <-ctx.Done():
					return
				case workerStream <- data:
					workerStream = nil
				case logStream <- data:
					logStream = nil
				}
			}
		}
	}()
	return workerStream, logStream
}
func sourceStream(ctx context.Context) chan interface{} {
	stream := make(chan interface{})
	go func() {
		defer close(stream)
		for n := range rand.Intn(10) {
			select {
			case <-ctx.Done():
				return
			case stream <- fmt.Sprintf("data: %v", n):
			}
		}
	}()
	return stream
}
func main() {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := sourceStream(ctx)
	workerStream, logStream := teeStream(ctx, stream)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for data := range workerStream {
			fmt.Printf("Processing: %v\n", data)
		}
	}()
	go func() {
		defer wg.Done()
		for data := range logStream {
			fmt.Printf("logging: %v\n", data)
		}
	}()
	wg.Wait()
	log.Println("Done!")
}
