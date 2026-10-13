# Arion codemode sandbox

This crate builds the QuickJS guest as a WebAssembly component and installs the
vendored artifact used by `arion-lib`.

## Connected build

```sh
./arion-sandbox/build.sh
```

The script downloads missing, pinned releases into `arion-sandbox/.build`, then
builds the core module, initializes QuickJS with Wizer, and componentizes it.
It selects the sandbox's pinned Rust toolchain and Wasm target flags regardless
of the directory it is invoked from, using the checked-in sandbox `Cargo.lock`.
The Cargo build and compiler metadata ignore inherited `RUSTFLAGS`,
`CARGO_ENCODED_RUSTFLAGS`, `RUSTUP_TOOLCHAIN`, and `CARGO_TARGET_DIR` so the proxy's
native build settings cannot change the guest build.

The vendored component is built from the Rust 2024 guest with the checked-in
lockfile and pinned tools below. `BUILD_INFO` records the edition, guest lockfile
SHA-256, tool versions, and compiler version. The artifact's checksum is stored
alongside it in `codemode_sandbox.wasm.sha256`; verify it from the artifact
directory with `sha256sum --check codemode_sandbox.wasm.sha256`.

## Offline build

Set `CODEMODE_OFFLINE=1` and provide either extracted tools/binaries:

```sh
CODEMODE_OFFLINE=1 \
WASI_SDK=/opt/codemode/wasi-sdk-25.0-x86_64-linux \
WIZER=/opt/codemode/bin/wizer \
WASM_TOOLS=/opt/codemode/bin/wasm-tools \
WASI_ADAPTER=/opt/codemode/wasi_snapshot_preview1.reactor.wasm \
./arion-sandbox/build.sh
```

or the unmodified release archives:

```sh
CODEMODE_OFFLINE=1 \
WASI_SDK_ARCHIVE=/opt/codemode/wasi-sdk-25.0-x86_64-linux.tar.gz \
WIZER_ARCHIVE=/opt/codemode/wizer-v11.0.3-x86_64-linux.tar.xz \
WASM_TOOLS_ARCHIVE=/opt/codemode/wasm-tools-1.243.0-x86_64-linux.tar.gz \
WASI_ADAPTER=/opt/codemode/wasi_snapshot_preview1.reactor.wasm \
./arion-sandbox/build.sh
```

`CODEMODE_BUILD_DIR` selects a different extraction/cache directory. Cargo also
runs with `--offline`, so its registry cache must already contain the locked
dependencies, or the repository must be configured to use a supplied vendored
source directory. The Rust 1.92.0 toolchain and `wasm32-wasip1` standard library
must likewise be installed in the offline environment.

The simplest repeatable bundle is a populated `arion-sandbox/.build` directory
from a connected build. Copy or archive that directory along with the Cargo
cache/vendor directory and installed Rust toolchain; after unpacking it at the
same path, the offline command is just `CODEMODE_OFFLINE=1 ./arion-sandbox/build.sh`.

See build.sh for pinned versions for WASI SDK, Wizer, wasm-tools, and Wasmtime. The resulting component, checksum, and build metadata are written to
`arion-lib/.../codemode/artifacts/`.
