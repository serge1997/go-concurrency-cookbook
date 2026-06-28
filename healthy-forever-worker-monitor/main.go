package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"time"
)

/*
Healthy Restart is a pattern used to monitor and automatically restart a goroutine
when it becomes unresponsive or finishes its work.

The workerMonitor acts as a steward — it watches the worker's heartbeat channel
and restarts it if no pulse is received within the restart timeout. This ensures
that long-running or unreliable tasks keep executing without manual intervention.

The worker signals it is alive by sending periodic pulses through the heartbeat
channel. If the worker finishes its task or times out, the monitor detects the
silence and spawns a new worker to take its place.

Key concepts:
  - Heartbeat: periodic signal proving the goroutine is alive
  - Steward:   monitors the heartbeat and restarts the worker when needed
  - Ward:      the worker being monitored
*/

type Worker func(context.Context, <-chan time.Time) chan struct{}

func sendPulse(ctx context.Context, c chan struct{}) {
	select {
	case <-ctx.Done():
	case c <- struct{}{}:
	default:
	}
}
func workerMonitor(ctx context.Context, worker Worker) {
	var workerHeartbeat chan struct{}
	restartTimeout := time.After(time.Second * 2)
	timeout := time.After(time.Second * 5)
	workerHeartbeat = worker(ctx, timeout)
monitorLoop:
	for {
		restartTimeout = time.After(time.Second * 2)
		timeout = time.After(time.Second * 5)
		select {
		case <-ctx.Done():
			return
		case _, ok := <-workerHeartbeat:
			if !ok {
				continue monitorLoop
			}
			log.Println("worker pulse received")
		case <-restartTimeout:
			log.Println("restarting worker")
			close(workerHeartbeat)
			workerHeartbeat = worker(ctx, timeout)
			continue monitorLoop
		}
	}
}

func worker(ctx context.Context, timeoutStream <-chan time.Time) chan struct{} {
	heartbeat := make(chan struct{})
	go func() {
		ticker := time.Tick(time.Second)
		doingWorkTime := time.After(time.Second * time.Duration(rand.Intn(6)))
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker:
				sendPulse(ctx, heartbeat)
			case <-timeoutStream:
				log.Println("worker: timeout")
				return
			case <-doingWorkTime:
				log.Println("worker done")
				return
			}
		}
	}()
	return heartbeat
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	workerMonitor(ctx, worker)
	<-ctx.Done()
	//cleanup some resource if  exists
	log.Println("Program interrupted")
}
