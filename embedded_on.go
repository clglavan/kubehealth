//go:build embed_model

package main

import (
	"context"
	"embed"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"time"
)

// Bundled assets — populated by `make fetch-embed-assets` before running
// `go build -tags embed_model`.
//
//go:embed embedded/model.gguf
//go:embed embedded/runtime
var embeddedAssets embed.FS

// embeddedModelName is the model id exposed to the OpenAI-compat clients.
const embeddedModelName = "magos-k8s-0.6b"

// Hardcoded for the bundled GGUF. If you ever swap the embedded model these
// need updating — they only feed the AI Agent modal's display fields.
const (
	embeddedArch         = "qwen3"
	embeddedQuantization = "Q8_0"
)

// State for the spawned llama-server. Held in package vars so the
// settings handler can restart it when the user changes context length.
var embServer struct {
	sync.Mutex
	cmd       *exec.Cmd
	cancel    context.CancelFunc
	port      int
	serverBin string
	modelPath string
	ctxSize   int
	running   bool
}

// isEmbedded reports whether this binary bundles a model (the embed_model build).
func isEmbedded() bool { return true }

func embeddedModelArch() string         { return embeddedArch }
func embeddedModelQuantization() string { return embeddedQuantization }

// embeddedRuntime exposes the live runtime state to the AI config endpoint.
func embeddedRuntime() (loadedCtx int, running bool) {
	embServer.Lock()
	defer embServer.Unlock()
	return embServer.ctxSize, embServer.running
}

// startEmbeddedLLM extracts the bundled llama-server + GGUF, launches it,
// and sets LMSTUDIO_URL/LMSTUDIO_MODEL so initAI() picks it up.
func startEmbeddedLLM() (func(), error) {
	if os.Getenv("LMSTUDIO_URL") != "" {
		log.Printf("Embedded LLM: LMSTUDIO_URL is set — not starting bundled llama-server")
		return func() {}, nil
	}

	base, err := bundleDir()
	if err != nil {
		return nil, err
	}
	log.Printf("Embedded LLM: bundle dir %s", base)

	runtimeDir := filepath.Join(base, "runtime")
	if err := extractDir(embeddedAssets, "embedded/runtime", runtimeDir); err != nil {
		return nil, fmt.Errorf("extract runtime: %w", err)
	}

	modelPath := filepath.Join(base, "model.gguf")
	if err := extractFile(embeddedAssets, "embedded/model.gguf", modelPath); err != nil {
		return nil, fmt.Errorf("extract model: %w", err)
	}

	serverBin := filepath.Join(runtimeDir, "llama-server")
	if runtime.GOOS == "windows" {
		serverBin += ".exe"
	}
	if _, err := os.Stat(serverBin); err != nil {
		return nil, fmt.Errorf("llama-server missing at %s: %w", serverBin, err)
	}
	if err := os.Chmod(serverBin, 0o755); err != nil {
		return nil, fmt.Errorf("chmod llama-server: %w", err)
	}

	port, err := freePort()
	if err != nil {
		return nil, fmt.Errorf("pick free port: %w", err)
	}

	embServer.Lock()
	embServer.serverBin = serverBin
	embServer.modelPath = modelPath
	embServer.port = port
	embServer.Unlock()

	if err := spawnLlamaServer(getEmbeddedCtxSize()); err != nil {
		return nil, err
	}

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	os.Setenv("LMSTUDIO_URL", baseURL+"/v1")
	if os.Getenv("LMSTUDIO_MODEL") == "" {
		os.Setenv("LMSTUDIO_MODEL", embeddedModelName)
	}
	log.Printf("Embedded LLM: ready at %s (model=%s)", baseURL, os.Getenv("LMSTUDIO_MODEL"))

	// Forward SIGINT/SIGTERM to llama-server so it doesn't outlive us.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Printf("Embedded LLM: shutting down llama-server")
		stopLlamaServer()
		os.Exit(0)
	}()

	return stopLlamaServer, nil
}

// spawnLlamaServer launches a fresh llama-server with the given context size
// on the previously chosen port and waits for it to become ready. Caller must
// have populated embServer.{serverBin, modelPath, port}.
func spawnLlamaServer(ctxSize int) error {
	embServer.Lock()
	bin := embServer.serverBin
	model := embServer.modelPath
	port := embServer.port
	embServer.Unlock()

	if bin == "" || model == "" || port == 0 {
		return fmt.Errorf("embedded server not initialised")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, bin,
		"-m", model,
		"--host", "127.0.0.1",
		"--port", fmt.Sprintf("%d", port),
		"--ctx-size", fmt.Sprintf("%d", ctxSize),
		"--n-gpu-layers", "999",
	)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	log.Printf("Embedded LLM: starting llama-server on 127.0.0.1:%d (ctx=%d)", port, ctxSize)
	if err := cmd.Start(); err != nil {
		cancel()
		return fmt.Errorf("start llama-server: %w", err)
	}

	if err := waitForReady(fmt.Sprintf("http://127.0.0.1:%d/v1/models", port), 60*time.Second); err != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
		cancel()
		return fmt.Errorf("llama-server did not become ready: %w", err)
	}

	embServer.Lock()
	embServer.cmd = cmd
	embServer.cancel = cancel
	embServer.ctxSize = ctxSize
	embServer.running = true
	embServer.Unlock()
	return nil
}

// stopLlamaServer terminates the running llama-server, if any.
func stopLlamaServer() {
	embServer.Lock()
	cmd := embServer.cmd
	cancel := embServer.cancel
	embServer.cmd = nil
	embServer.cancel = nil
	embServer.running = false
	embServer.Unlock()

	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
	if cancel != nil {
		cancel()
	}
	_ = cmd.Wait()
}

// restartEmbedded shuts the current llama-server down and starts a new one
// with the given context size. Persists the new value via settings.
func restartEmbedded(ctxSize int) error {
	setEmbeddedCtxSize(ctxSize)
	stopLlamaServer()
	return spawnLlamaServer(getEmbeddedCtxSize())
}

// bundleDir picks the on-disk location where we extract the embedded assets.
func bundleDir() (string, error) {
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("locate user cache dir: %w", err)
	}
	dir := filepath.Join(cacheDir, "kubehealth", "embedded")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("create cache dir: %w", err)
	}
	return dir, nil
}

func extractFile(fsys embed.FS, src, dest string) error {
	srcFile, err := fsys.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	stat, err := srcFile.Stat()
	if err != nil {
		return err
	}
	if existing, err := os.Stat(dest); err == nil && existing.Size() == stat.Size() {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	tmp := dest + ".tmp"
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, srcFile); err != nil {
		out.Close()
		os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, dest)
}

func extractDir(fsys embed.FS, srcRoot, destRoot string) error {
	return fs.WalkDir(fsys, srcRoot, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel := p[len(srcRoot):]
		if rel == "" {
			return os.MkdirAll(destRoot, 0o755)
		}
		out := filepath.Join(destRoot, filepath.FromSlash(rel))
		if d.IsDir() {
			return os.MkdirAll(out, 0o755)
		}
		return extractFile(fsys, p, out)
	})
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func waitForReady(url string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 1 * time.Second}
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode < 500 {
				return nil
			}
			lastErr = fmt.Errorf("status %d", resp.StatusCode)
		} else {
			lastErr = err
		}
		time.Sleep(250 * time.Millisecond)
	}
	if lastErr != nil {
		return fmt.Errorf("not ready within %s: %v", timeout, lastErr)
	}
	return fmt.Errorf("not ready within %s", timeout)
}
