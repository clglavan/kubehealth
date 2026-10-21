package main

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// What KubeHealth started (tunnels, the embedded llama-server) is stopped
// when it exits: on SIGINT or SIGTERM, or when the server fails.
var shutdownHooks = struct {
	sync.Mutex
	fns  []func()
	done bool
}{}

// onShutdown registers fn to run when KubeHealth exits.
func onShutdown(fn func()) {
	shutdownHooks.Lock()
	defer shutdownHooks.Unlock()
	shutdownHooks.fns = append(shutdownHooks.fns, fn)
}

// runShutdown runs the hooks once, newest first.
func runShutdown() {
	shutdownHooks.Lock()
	defer shutdownHooks.Unlock()
	if shutdownHooks.done {
		return
	}
	shutdownHooks.done = true
	for i := len(shutdownHooks.fns) - 1; i >= 0; i-- {
		shutdownHooks.fns[i]()
	}
}

// handleSignals runs the hooks and exits on SIGINT or SIGTERM.
func handleSignals() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-ch
		log.Printf("Shutting down")
		runShutdown()
		os.Exit(0)
	}()
}
