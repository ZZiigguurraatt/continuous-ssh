## continuous-ssh (xssh) — Makefile

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN := $(shell go env GOPATH)/bin
else
GOBIN := $(shell go env GOBIN)
endif

# Build output goes into bin/.
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p "$(LOCALBIN)"

PKG := ./cmd/xssh
NATIVE := $(LOCALBIN)/xssh
PI64 := $(LOCALBIN)/xssh-arm64
PI32 := $(LOCALBIN)/xssh-armv7
PI_ZERO := $(LOCALBIN)/xssh-armv6

# Install locations.
#   USER_BIN defaults to whatever `go env GOBIN` (or GOPATH/bin) reports —
#   i.e. the same directory `go install` would drop binaries into. Override
#   if you want elsewhere, e.g. USER_BIN=$HOME/bin.
USER_BIN ?= $(GOBIN)
SYSTEM_BIN ?= /usr/local/bin

# Shell-completion install locations. Override on the command line if your
# distro stores them elsewhere.
USER_BASH_COMPDIR   ?= $(HOME)/.local/share/bash-completion/completions
USER_ZSH_COMPDIR    ?= $(HOME)/.local/share/zsh/site-functions
SYSTEM_BASH_COMPDIR ?= /usr/share/bash-completion/completions
SYSTEM_ZSH_COMPDIR  ?= /usr/share/zsh/site-functions

BASH_COMPLETION := completions/bash/xssh
ZSH_COMPLETION  := completions/zsh/_xssh

# Remote deploy parameters — override on the command line.
HOST ?= user@host

# Where to scp the binary on the remote.
#   - If REMOTE_PATH is set on the command line, that value wins.
#   - Else, for root@ hosts: /usr/local/bin/ (scp has no way to elevate
#     so a non-root user can't write there, but root can).
#   - Else, an ssh probe asks the REMOTE for its preferred dir, using
#     the same logic our local USER_BIN does:
#         GOBIN if non-empty, else GOPATH/bin, else $HOME/go/bin.
#     Whatever path comes back is also `mkdir -p`'d in the same ssh
#     call so the probe + dir-create + scp adds up to just two remote
#     connections per `make deploy*`. The probe runs inside the recipe
#     (not at parse time), so `make help` etc. never trigger ssh.
REMOTE_PATH ?=

.PHONY: all
all: build

##@ General

# `make help` auto-generates a categorised list of targets by scanning for
# lines of the form `target: ## description` and `##@ category` headers.
.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Build

# Build targets are .PHONY because `go build` already has a content-aware
# cache: re-running it is free when nothing changed, and reliable when
# something did. Letting Make second-guess (via file-mtime rules against
# the bin/ directory) made `make build` silently skip rebuilds whenever
# bin/xssh already existed.
.PHONY: build
build: | $(LOCALBIN) ## Build native binary into bin/xssh.
	go build -o $(NATIVE) $(PKG)

.PHONY: pi64
pi64: | $(LOCALBIN) ## Cross-compile for Raspberry Pi 3/4/5 (64-bit OS) into bin/xssh-arm64.
	GOOS=linux GOARCH=arm64 go build -o $(PI64) $(PKG)

.PHONY: pi32
pi32: | $(LOCALBIN) ## Cross-compile for Pi 2 / Pi 3 (32-bit OS) into bin/xssh-armv7.
	GOOS=linux GOARCH=arm GOARM=7 go build -o $(PI32) $(PKG)

.PHONY: pi-zero
pi-zero: | $(LOCALBIN) ## Cross-compile for Pi 1 / Pi Zero (ARMv6) into bin/xssh-armv6.
	GOOS=linux GOARCH=arm GOARM=6 go build -o $(PI_ZERO) $(PKG)

.PHONY: clean
clean: ## Remove built binaries.
	rm -rf $(LOCALBIN)

##@ Install (local)

.PHONY: install-user
install-user: build ## Install xssh + shell completions into per-user locations (no sudo).
	@mkdir -p "$(USER_BIN)"
	@install -m 0755 $(NATIVE) "$(USER_BIN)/xssh"
	@echo "Installed xssh to $(USER_BIN)/xssh"
	@mkdir -p "$(USER_BASH_COMPDIR)" "$(USER_ZSH_COMPDIR)"
	@install -m 0644 $(BASH_COMPLETION) "$(USER_BASH_COMPDIR)/xssh"
	@install -m 0644 $(ZSH_COMPLETION) "$(USER_ZSH_COMPDIR)/_xssh"
	@echo "Installed bash completion to $(USER_BASH_COMPDIR)/xssh"
	@echo "Installed zsh completion to $(USER_ZSH_COMPDIR)/_xssh"
	@case ":$$PATH:" in *":$(USER_BIN):"*) ;; *) echo "Note: $(USER_BIN) is not in your PATH." ;; esac
	@echo "Note: for zsh completion, ensure $(USER_ZSH_COMPDIR) is in \$$fpath before compinit."

.PHONY: install-system
install-system: build ## Install xssh + shell completions into system locations (requires sudo).
	@sudo install -m 0755 $(NATIVE) "$(SYSTEM_BIN)/xssh"
	@echo "Installed xssh to $(SYSTEM_BIN)/xssh"
	@sudo install -d -m 0755 "$(SYSTEM_BASH_COMPDIR)" "$(SYSTEM_ZSH_COMPDIR)"
	@sudo install -m 0644 $(BASH_COMPLETION) "$(SYSTEM_BASH_COMPDIR)/xssh"
	@sudo install -m 0644 $(ZSH_COMPLETION) "$(SYSTEM_ZSH_COMPDIR)/_xssh"
	@echo "Installed bash completion to $(SYSTEM_BASH_COMPDIR)/xssh"
	@echo "Installed zsh completion to $(SYSTEM_ZSH_COMPDIR)/_xssh"

.PHONY: uninstall-user
uninstall-user: ## Remove xssh + completions from per-user locations.
	@rm -f "$(USER_BIN)/xssh" "$(USER_BASH_COMPDIR)/xssh" "$(USER_ZSH_COMPDIR)/_xssh"
	@echo "Removed $(USER_BIN)/xssh + completions"

.PHONY: uninstall-system
uninstall-system: ## Remove xssh + completions from system locations (requires sudo).
	@sudo rm -f "$(SYSTEM_BIN)/xssh" "$(SYSTEM_BASH_COMPDIR)/xssh" "$(SYSTEM_ZSH_COMPDIR)/_xssh"
	@echo "Removed $(SYSTEM_BIN)/xssh + completions"

##@ Deploy (remote)

# `deploy*` targets scp the built binary onto $(HOST). The destination
# directory is auto-detected by probing the remote for its Go env (see
# RESOLVE_REMOTE_PATH above). Override with REMOTE_PATH=... to skip
# the probe entirely.
#   make deploy        HOST=user@host
#   make deploy-pi64   HOST=pi@pi.local
#   make deploy        HOST=user@host REMOTE_PATH=~/.local/bin/

# do_deploy: resolve the remote install dir (one of: REMOTE_PATH
# override / /usr/local/bin/ for root@ / ssh-probe-of-remote-Go-env),
# `mkdir -p` it on the remote, then scp the binary. $(1) is the local
# file to push.
define do_deploy
@if [ -n "$(REMOTE_PATH)" ]; then \
	    dest="$(REMOTE_PATH)"; \
	    ssh "$(HOST)" "mkdir -p \"$${dest%/}/\""; \
	elif echo "$(HOST)" | grep -q '^root@'; then \
	    dest=/usr/local/bin/; \
	else \
	    dest=$$(ssh "$(HOST)" 'if command -v go >/dev/null 2>&1; then \
	        gobin=$$(go env GOBIN); \
	        if [ -n "$$gobin" ]; then dest="$$gobin"; \
	        else dest="$$(go env GOPATH)/bin"; fi; \
	    else dest="$$HOME/go/bin"; fi; \
	    mkdir -p "$$dest" && printf "%s" "$$dest"'); \
	fi; \
	dest="$${dest%/}/"; \
	echo "→ deploying $(1) to $(HOST):$${dest}xssh"; \
	scp $(1) "$(HOST):$${dest}xssh"
endef

.PHONY: deploy
deploy: build ## Native build → scp to the remote's auto-detected bindir.
	$(call do_deploy,$(NATIVE))

.PHONY: deploy-pi64
deploy-pi64: pi64 ## ARM64 cross-build → scp to the remote's auto-detected bindir.
	$(call do_deploy,$(PI64))

.PHONY: deploy-pi32
deploy-pi32: pi32 ## ARMv7 cross-build → scp to the remote's auto-detected bindir.
	$(call do_deploy,$(PI32))

.PHONY: deploy-pi-zero
deploy-pi-zero: pi-zero ## ARMv6 cross-build → scp to the remote's auto-detected bindir.
	$(call do_deploy,$(PI_ZERO))
