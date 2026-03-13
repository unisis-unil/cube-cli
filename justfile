# cube — UNISIS S3 Cubes CLI

default:
    @just --list

# Build in debug mode
build:
    cargo build

# Build optimized release binary
release:
    cargo build --release

# Run all tests
test:
    cargo test

# Install cube to ~/.cargo/bin/
install:
    cargo install --path .

# Uninstall cube
uninstall:
    cargo uninstall cube

# Sync cubes from GCS
sync:
    cargo run --quiet -- sync

# List available cubes
schema:
    cargo run --quiet -- schema

# Run cube with arbitrary arguments
run *ARGS:
    cargo run --quiet -- {{ARGS}}

# Check code (clippy + fmt)
check:
    cargo fmt -- --check
    cargo clippy -- -D warnings

# Format code
fmt:
    cargo fmt

# Bump version (patch, minor, or major), commit with jj, tag, and push
bump level:
    #!/usr/bin/env bash
    set -euo pipefail
    CURRENT=$(grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)".*/\1/')
    IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT"
    case "{{level}}" in
        patch) PATCH=$((PATCH + 1)) ;;
        minor) MINOR=$((MINOR + 1)); PATCH=0 ;;
        major) MAJOR=$((MAJOR + 1)); MINOR=0; PATCH=0 ;;
        *) echo "Usage: just bump [patch|minor|major]"; exit 1 ;;
    esac
    NEW="${MAJOR}.${MINOR}.${PATCH}"
    echo "Version: ${CURRENT} → ${NEW}"
    # Update Cargo.toml
    sed -i'' -e "s/^version = \"${CURRENT}\"/version = \"${NEW}\"/" Cargo.toml
    # Update Cargo.lock
    cargo generate-lockfile --quiet
    # Commit with jj
    jj describe -m "release: v${NEW}"
    jj new
    jj bookmark set main -r @-
    # Tag via git (jj doesn't support tags)
    jj git export
    git tag "v${NEW}"
    echo ""
    echo "Version ${NEW} prête."
    echo "Pour publier : jj git push && git push origin v${NEW}"
