# embedded/

This directory is populated at build time by `make fetch-assets` (or
`make embed`). Its contents are NOT committed to git — they're large
binary blobs (the model is ~600 MB).

After `make fetch-assets` you should have:

```
embedded/
  model.gguf              # magos-k8s-0.6b quantized weights
  runtime/
    llama-server          # llama.cpp server binary (platform-specific)
    libllama.dylib        # macOS shared libraries (or .so on Linux)
    libggml*.dylib
    ...
```

When `go build -tags embed_model` runs, the `//go:embed` directives in
`embedded_on.go` pull these files into the final binary.
