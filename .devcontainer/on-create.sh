#!/bin/bash
set -e

# This script is executed as root by default.



echo "Executing post-creation script..."
# 2. Run the original Python environment setup
pipx install uv && uv venv --allow-existing && uv sync

# 1. Restore Gemini credentials if they exist

CRED_SOURCE_DIR="/workspaces/kafka-tutorials/.devcontainer/.persisted-credentials"
CRED_SOURCE_FILE="${CRED_SOURCE_DIR}/credentials.json"
CRED_DEST_DIR="/home/vscode/.cache/google-vscode-extension/auth"
CRED_DEST_FILE="${CRED_DEST_DIR}/credentials.json"

if [ -f "$CRED_SOURCE_FILE" ]; then
    echo "Restoring Gemini credentials..."
    mkdir -p "$CRED_DEST_DIR"
    cp "$CRED_SOURCE_DIR"/* "$CRED_DEST_DIR"
    # Ensure the 'vscode' user owns the restored files, not root
    chown -R vscode:vscode "/home/vscode/.cache"
    echo "Credentials restored and permissions set."
fi

