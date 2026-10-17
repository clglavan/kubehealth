//go:build !embed_model

package main

import "errors"

// startEmbeddedLLM is a no-op in non-embedded builds.
// The embedded build (go build -tags embed_model) replaces this with the
// version in embedded_on.go that bundles a model and spawns llama-server.
func startEmbeddedLLM() (cleanup func(), err error) {
	return func() {}, nil
}

// isEmbedded reports whether this binary bundles a model.
func isEmbedded() bool { return false }

func embeddedModelArch() string         { return "" }
func embeddedModelQuantization() string { return "" }

// embeddedRuntime returns zero values in non-embedded builds.
func embeddedRuntime() (loadedCtx int, running bool) { return 0, false }

// restartEmbedded is rejected in non-embedded builds — the user should change
// context length in their LM Studio / Ollama instance directly.
func restartEmbedded(ctxSize int) error {
	return errors.New("not an embedded build — change context length in LM Studio instead")
}
