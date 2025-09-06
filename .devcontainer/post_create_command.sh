#!/bin/bash

curl -fsSL https://dl.dagger.io/dagger/install.sh | DAGGER_VERSION=0.18.17 BIN_DIR=$HOME/.local/bin sh
mkdir -p /home/vscode/.local/share/bash-completion/completions
dagger completion bash > /home/vscode/.local/share/bash-completion/completions/dagger
