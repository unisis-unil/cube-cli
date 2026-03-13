# cube — UNISIS Data Warehouse CLI

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
