.PHONY: help build build-embed embed fetch-assets fetch-model fetch-runtime clean clean-assets run release release-embed

# ---- Configuration --------------------------------------------------------

GOOS  ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)

# Version stamped into the binary (kubehealth -version).
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

# Platforms for `make release` (the regular build is pure Go, so it cross-compiles).
RELEASE_PLATFORMS ?= darwin/arm64 darwin/amd64 linux/amd64 linux/arm64 windows/amd64

# llama.cpp release to bundle. Update this when you want a newer runtime.
# Verified asset layout for this tag: archive root is `llama-$(LLAMA_RELEASE)/`
# with llama-server + dylibs at the top level.
LLAMA_RELEASE ?= b9305

# Platform-specific llama.cpp asset names (from github.com/ggerganov/llama.cpp/releases)
ifeq ($(GOOS)/$(GOARCH),darwin/arm64)
  LLAMA_ASSET := llama-$(LLAMA_RELEASE)-bin-macos-arm64.tar.gz
endif
ifeq ($(GOOS)/$(GOARCH),darwin/amd64)
  LLAMA_ASSET := llama-$(LLAMA_RELEASE)-bin-macos-x64.tar.gz
endif
ifeq ($(GOOS)/$(GOARCH),linux/amd64)
  LLAMA_ASSET := llama-$(LLAMA_RELEASE)-bin-ubuntu-x64.tar.gz
endif
ifeq ($(GOOS)/$(GOARCH),linux/arm64)
  LLAMA_ASSET := llama-$(LLAMA_RELEASE)-bin-ubuntu-arm64.tar.gz
endif

# Source for the magos GGUF. Downloads from HuggingFace by default, pinned to a
# model release tag so an embed build always bundles the same model (bump
# MAGOS_REVISION with each model release).
# Override with `make MODEL_URL=...` to use a different quantization (q4_k_m, f16)
# or `make MODEL_SRC=/path/to.gguf` to use a local file (skips the download).
MAGOS_REVISION ?= v19
MODEL_URL ?= https://huggingface.co/clglavan/magos-k8s-0.6b/resolve/$(MAGOS_REVISION)/magos-k8s-0.6b-q8_0.gguf
MODEL_SRC ?=

# ---- Targets --------------------------------------------------------------

help:
	@echo "kubehealth build targets"
	@echo ""
	@echo "  make build         Build the regular binary  →  ./kubehealth"
	@echo "  make embed         Fetch assets + build the embedded binary  →  ./kubehealth-embed"
	@echo "  make fetch-assets  Just stage the model + llama-server into embedded/"
	@echo "  make run           go run the regular binary"
	@echo "  make release       Cross-compile the regular binary for every platform  →  dist/"
	@echo "  make release-embed Package this platform's embedded binary  →  dist/"
	@echo "  make clean         Remove built binaries"
	@echo "  make clean-assets  Remove embedded/ assets (forces re-fetch)"
	@echo ""
	@echo "Variables:"
	@echo "  GOOS=$(GOOS)  GOARCH=$(GOARCH)  LLAMA_RELEASE=$(LLAMA_RELEASE)  VERSION=$(VERSION)"
	@echo "  MODEL_URL=$(MODEL_URL)"
	@echo "  MODEL_SRC=$(MODEL_SRC)   (set to skip the download and copy a local .gguf)"

build:
	go build -ldflags "-X main.version=$(VERSION)" -o kubehealth .

build-embed:
	@if [ ! -f embedded/model.gguf ] || [ ! -f embedded/runtime/llama-server ]; then \
		echo "embedded/ assets missing — run 'make fetch-assets' first"; \
		exit 1; \
	fi
	go build -tags embed_model -ldflags "-X main.version=$(VERSION)" -o kubehealth-embed .

embed: fetch-assets build-embed

fetch-assets: fetch-model fetch-runtime

fetch-model: embedded/model.gguf

embedded/model.gguf:
	@mkdir -p embedded
	@if [ -n "$(MODEL_SRC)" ]; then \
		if [ ! -f "$(MODEL_SRC)" ]; then \
			echo "ERROR: MODEL_SRC=$(MODEL_SRC) does not exist"; exit 1; \
		fi; \
		echo "Copying $(MODEL_SRC) -> embedded/model.gguf"; \
		cp "$(MODEL_SRC)" embedded/model.gguf; \
	else \
		echo "Downloading $(MODEL_URL)"; \
		curl -fL --progress-bar -o embedded/model.gguf.tmp "$(MODEL_URL)"; \
		mv embedded/model.gguf.tmp embedded/model.gguf; \
	fi

fetch-runtime: embedded/runtime/llama-server

embedded/runtime/llama-server:
	@if [ -z "$(LLAMA_ASSET)" ]; then \
		echo "ERROR: no llama.cpp asset known for $(GOOS)/$(GOARCH)."; \
		echo "  Add a mapping in the Makefile or supply embedded/runtime/ yourself."; \
		exit 1; \
	fi
	@mkdir -p embedded/runtime
	@echo "Downloading $(LLAMA_ASSET)"
	curl -fL --progress-bar -o /tmp/$(LLAMA_ASSET) \
		https://github.com/ggerganov/llama.cpp/releases/download/$(LLAMA_RELEASE)/$(LLAMA_ASSET)
	@echo "Extracting llama-server + shared libs"
	tar -xzf /tmp/$(LLAMA_ASSET) -C embedded/runtime/ --strip-components=1
	@echo "Dereferencing dylib symlinks (//go:embed silently skips them)"
	@find embedded/runtime -type l | while read link; do \
		cp -L "$$link" "$$link.real" && rm "$$link" && mv "$$link.real" "$$link"; \
	done
	chmod +x embedded/runtime/llama-server
	rm -f /tmp/$(LLAMA_ASSET)

run:
	go run .

# -trimpath keeps local paths (your home directory) out of the binaries.
release:
	@rm -rf dist/kubehealth_$(VERSION)_* && mkdir -p dist
	@for p in $(RELEASE_PLATFORMS); do \
		os=$${p%/*}; arch=$${p#*/}; ext=""; [ "$$os" = windows ] && ext=".exe"; \
		dir="kubehealth_$(VERSION)_$${os}_$${arch}"; \
		echo "Building $$dir"; \
		CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch go build -trimpath -ldflags "$(LDFLAGS)" -o "dist/$$dir/kubehealth$$ext" . || exit 1; \
		cp README.md "dist/$$dir/"; \
		if [ "$$os" = windows ]; then (cd dist && zip -qr "$$dir.zip" "$$dir"); \
		else tar -C dist -czf "dist/$$dir.tar.gz" "$$dir"; fi; \
		rm -rf "dist/$$dir"; \
	done
	@cd dist && shasum -a 256 kubehealth_$(VERSION)_* > checksums-$(VERSION).txt && cat checksums-$(VERSION).txt

release-embed:
	@if [ ! -f embedded/model.gguf ] || [ ! -f embedded/runtime/llama-server ]; then \
		echo "embedded/ assets missing — run 'make fetch-assets' first"; exit 1; \
	fi
	@mkdir -p dist
	@dir="kubehealth-embed_$(VERSION)_$(GOOS)_$(GOARCH)"; \
		echo "Building $$dir"; \
		go build -trimpath -tags embed_model -ldflags "$(LDFLAGS)" -o "dist/$$dir/kubehealth-embed" . && \
		cp README.md "dist/$$dir/" && \
		tar -C dist -czf "dist/$$dir.tar.gz" "$$dir" && rm -rf "dist/$$dir" && \
		cd dist && shasum -a 256 "$$dir.tar.gz" | tee -a checksums-$(VERSION).txt

clean:
	rm -f kubehealth kubehealth-embed
	rm -rf dist

clean-assets:
	rm -rf embedded/model.gguf embedded/runtime
