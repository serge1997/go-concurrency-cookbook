package main

/*
Heartbeats are a way for concurrent processes to signal life to outside parties.
When a goroutine is doing a heavy task that take too many time, the heartbeat signal must
be something interresting to signal that the task is running.
*/
import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"
)

func worker(ctx context.Context, stream <-chan interface{}, timeout time.Duration) (<-chan interface{}, <-chan interface{}) {
	heartbeat := make(chan interface{})
	result := make(chan interface{})
	go func() {
		defer close(heartbeat)
		defer close(result)
		pulse := time.Tick(time.Second)
		sendPulse := func() {
			select {
			case heartbeat <- struct{}{}:
			default:
			}
		}
		execWorker := func(data interface{}) {
			for {
				select {
				case <-ctx.Done():
					return
				case <-pulse:
					sendPulse()
				case result <- fmt.Sprintf("worker result: %v", data):
					return
				}
			}

		}
		for {
			timedOut := time.After(timeout)
			select {
			case <-ctx.Done():
				return
			case <-pulse:
				sendPulse()
			case <-timedOut:
				log.Println("timeout")
				return
			case data, ok := <-stream:
				if !ok {
					return
				}
				execWorker(data)
			}
		}
	}()
	return heartbeat, result
}

var genStream = func(ctx context.Context) <-chan interface{} {
	stream := make(chan interface{})
	go func() {
		defer close(stream)
		for n := range rand.Intn(20) {
			select {
			case <-ctx.Done():
				return
			case stream <- n:
			}
			time.Sleep(time.Second * time.Duration(rand.Intn(4)))
		}
	}()
	return stream
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	timeout := time.Second * 30
	strem := genStream(ctx)
	heartbeat, result := worker(ctx, strem, timeout)

	for {
		timedOut := time.After(timeout)
		select {
		case _, ok := <-heartbeat:
			if !ok {
				return
			}
			log.Println("heartbeat received")
		case r, ok := <-result:
			if !ok {
				return
			}
			log.Println(r)
		case <-timedOut:
			return
		}
	}
}
