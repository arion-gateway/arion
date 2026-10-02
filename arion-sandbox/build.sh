#!/usr/bin/env bash
set -euo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
readonly BUILD_DIR="${CODEMODE_BUILD_DIR:-${SCRIPT_DIR}/.build}"
readonly ARTIFACT_DIR="${REPO_DIR}/arion-lib/src/listeners/http_connection_manager/mcp_gateway/codemode/artifacts"
readonly TARGET="wasm32-wasip1"

readonly WASI_SDK_VERSION="25.0"
readonly WIZER_VERSION="11.0.3"
readonly WASM_TOOLS_VERSION="1.243.0"
readonly WASMTIME_VERSION="40.0.4"
readonly RQUICKJS_VERSION="0.12.0"
readonly WIT_BINDGEN_VERSION="0.57.1"

case "${CODEMODE_OFFLINE:-0}" in
  1|true|yes) readonly OFFLINE=true ;;
  0|false|no) readonly OFFLINE=false ;;
  *) echo "CODEMODE_OFFLINE must be 0 or 1" >&2; exit 1 ;;
esac

host_arch="$(uname -m)"
case "${host_arch}" in
  x86_64)
    tool_arch="x86_64"
    wasi_arch="x86_64"
    wasi_sdk_sha256="52640dde13599bf127a95499e61d6d640256119456d1af8897ab6725bcf3d89c"
    wizer_sha256="36016e7f846f38ad09f22eb4bafb99068f038c431eb9613ddf59e31ad8a21414"
    wasm_tools_sha256="f261622f8015d38ebe9c3345cc2f7bb5de055d3a66ab44efdf78f11068ed9d9f"
    ;;
  aarch64|arm64)
    tool_arch="aarch64"
    wasi_arch="arm64"
    wasi_sdk_sha256="${WASI_SDK_SHA256:-}"
    wizer_sha256="73bffcbaf736ef24109a48cac431d070c43cb2ee8a14a840576f7aec0133d8d6"
    wasm_tools_sha256="ad06ba3c527992a1e6e9a7e807cc2bb914072f0a0ae6ce71680de91b1054d2e9"
    ;;
  *) echo "unsupported host architecture: ${host_arch}" >&2; exit 1 ;;
esac

readonly WASI_ADAPTER_SHA256="1665c60ef6e1c08837a531a62747d942005cf40984b5c48e52cf34e0c75baf87"

# Remote artifact locations. Override the URL variables to use an internal
# mirror, or the path variables when the artifacts are already cached locally.
readonly WASI_SDK_RELEASE="wasi-sdk-${WASI_SDK_VERSION}-${wasi_arch}-linux"
readonly WASI_SDK_PATH="${WASI_SDK:-${BUILD_DIR}/${WASI_SDK_RELEASE}}"
readonly WASI_SDK_ARCHIVE="${WASI_SDK_ARCHIVE:-${BUILD_DIR}/${WASI_SDK_RELEASE}.tar.gz}"
readonly WASI_SDK_URL="${WASI_SDK_URL:-https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-25/${WASI_SDK_RELEASE}.tar.gz}"

readonly WIZER_RELEASE="wizer-v${WIZER_VERSION}-${tool_arch}-linux"
readonly WIZER_PATH="${WIZER:-${BUILD_DIR}/${WIZER_RELEASE}/wizer}"
readonly WIZER_ARCHIVE="${WIZER_ARCHIVE:-${BUILD_DIR}/${WIZER_RELEASE}.tar.xz}"
readonly WIZER_URL="${WIZER_URL:-https://github.com/bytecodealliance/wizer/releases/download/v${WIZER_VERSION}/${WIZER_RELEASE}.tar.xz}"

readonly WASM_TOOLS_RELEASE="wasm-tools-${WASM_TOOLS_VERSION}-${tool_arch}-linux"
readonly WASM_TOOLS_PATH="${WASM_TOOLS:-${BUILD_DIR}/${WASM_TOOLS_RELEASE}/wasm-tools}"
readonly WASM_TOOLS_ARCHIVE="${WASM_TOOLS_ARCHIVE:-${BUILD_DIR}/${WASM_TOOLS_RELEASE}.tar.gz}"
readonly WASM_TOOLS_URL="${WASM_TOOLS_URL:-https://github.com/bytecodealliance/wasm-tools/releases/download/v${WASM_TOOLS_VERSION}/${WASM_TOOLS_RELEASE}.tar.gz}"

readonly WASI_ADAPTER_PATH="${WASI_ADAPTER:-${BUILD_DIR}/wasi_snapshot_preview1.reactor.wasm}"
readonly WASI_ADAPTER_URL="${WASI_ADAPTER_URL:-https://github.com/bytecodealliance/wasmtime/releases/download/v${WASMTIME_VERSION}/wasi_snapshot_preview1.reactor.wasm}"

verify_sha256() {
  local expected="$1"
  local path="$2"

  # WASI SDK 25 predates GitHub's published asset digests. Its arm64 hash can
  # be supplied explicitly while x86_64 uses the pinned hash above.
  [[ -z "${expected}" ]] && return

  local actual
  actual="$(sha256sum "${path}")"
  actual="${actual%% *}"
  if [[ "${actual}" != "${expected}" ]]; then
    echo "checksum mismatch for ${path}" >&2
    echo "expected ${expected}, got ${actual}" >&2
    exit 1
  fi
}

fetch() {
  local name="$1"
  local url="$2"
  local path="$3"
  local sha256="$4"

  if [[ ! -f "${path}" ]]; then
    if ${OFFLINE}; then
      echo "offline build requires ${name}: ${path}" >&2
      exit 1
    fi
    command -v curl >/dev/null 2>&1 || {
      echo "curl is required to download ${name}" >&2
      exit 1
    }
    echo "Downloading ${name}"
    curl --fail --location --output "${path}" "${url}"
  fi
  verify_sha256 "${sha256}" "${path}"
}

mkdir -p "${BUILD_DIR}"

readonly wasi_sdk="${WASI_SDK_PATH}"
if [[ -z "${WASI_SDK:-}" && ! -x "${wasi_sdk}/bin/clang" ]]; then
  fetch "WASI SDK ${WASI_SDK_VERSION}" \
    "${WASI_SDK_URL}" "${WASI_SDK_ARCHIVE}" "${wasi_sdk_sha256}"
  tar -xzf "${WASI_SDK_ARCHIVE}" -C "${BUILD_DIR}"
fi
[[ -x "${wasi_sdk}/bin/clang" ]] || {
  echo "WASI SDK not found at ${wasi_sdk}" >&2
  exit 1
}

readonly wizer="${WIZER_PATH}"
if [[ -z "${WIZER:-}" && ! -x "${wizer}" ]]; then
  fetch "Wizer ${WIZER_VERSION}" \
    "${WIZER_URL}" "${WIZER_ARCHIVE}" "${wizer_sha256}"
  tar -xJf "${WIZER_ARCHIVE}" -C "${BUILD_DIR}"
fi
[[ -x "${wizer}" ]] || { echo "Wizer is not executable: ${wizer}" >&2; exit 1; }

readonly wasm_tools="${WASM_TOOLS_PATH}"
if [[ -z "${WASM_TOOLS:-}" && ! -x "${wasm_tools}" ]]; then
  fetch "wasm-tools ${WASM_TOOLS_VERSION}" \
    "${WASM_TOOLS_URL}" "${WASM_TOOLS_ARCHIVE}" "${wasm_tools_sha256}"
  tar -xzf "${WASM_TOOLS_ARCHIVE}" -C "${BUILD_DIR}"
fi
[[ -x "${wasm_tools}" ]] || { echo "wasm-tools is not executable: ${wasm_tools}" >&2; exit 1; }

fetch "Wasmtime ${WASMTIME_VERSION} WASI reactor adapter" \
  "${WASI_ADAPTER_URL}" "${WASI_ADAPTER_PATH}" "${WASI_ADAPTER_SHA256}"

# Cargo runs in the sandbox directory, so preserve caller-relative SDK overrides.
export WASI_SDK
WASI_SDK="$(cd -- "${wasi_sdk}" && pwd)"
cargo_args=(
  build
  --manifest-path "${SCRIPT_DIR}/Cargo.toml"
  --release
  --target "${TARGET}"
  --locked
)
${OFFLINE} && cargo_args+=(--offline)
# Select the sandbox's pinned toolchain and target flags without changing how
# caller-relative paths are resolved by the remaining build steps.
(
  cd -- "${SCRIPT_DIR}"
  cargo "${cargo_args[@]}"
)

core_module="${SCRIPT_DIR}/target/${TARGET}/release/arion_codemode_sandbox.wasm"
initialized_module="${BUILD_DIR}/codemode_sandbox.core.wasm"
component="${BUILD_DIR}/codemode_sandbox.component.wasm"

"${wizer}" --allow-wasi --init-func wizer.initialize "${core_module}" -o "${initialized_module}"
"${wasm_tools}" component new "${initialized_module}" --adapt "${WASI_ADAPTER_PATH}" -o "${component}"

mkdir -p "${ARTIFACT_DIR}"
install -m 0644 "${component}" "${ARTIFACT_DIR}/codemode_sandbox.wasm"
artifact_hash="$(sha256sum "${ARTIFACT_DIR}/codemode_sandbox.wasm")"
echo "${artifact_hash%% *}  codemode_sandbox.wasm" > "${ARTIFACT_DIR}/codemode_sandbox.wasm.sha256"

{
  echo "sandbox=arion-codemode-sandbox@0.1.0"
  echo "component=arion:codemode/sandbox@0.1.0"
  echo "rquickjs=${RQUICKJS_VERSION}"
  echo "wit-bindgen=${WIT_BINDGEN_VERSION}"
  echo "target=${TARGET}"
  echo "wasi-sdk=${WASI_SDK_VERSION}"
  echo "wizer=${WIZER_VERSION}"
  echo "wasm-tools=${WASM_TOOLS_VERSION}"
  echo "wasi-adapter=wasmtime-${WASMTIME_VERSION}"
  echo "rustc=$(cd -- "${SCRIPT_DIR}" && rustc --version)"
} > "${ARTIFACT_DIR}/BUILD_INFO"

echo "Built ${ARTIFACT_DIR}/codemode_sandbox.wasm"
