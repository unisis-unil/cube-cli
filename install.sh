#!/bin/sh
set -e

REPO="unisis-unil/cube-cli"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"

# Detect platform
OS="$(uname -s)"
ARCH="$(uname -m)"

case "${OS}" in
  Darwin)
    case "${ARCH}" in
      x86_64) TARGET="cube-x86_64-apple-darwin" ;;
      arm64)  TARGET="cube-aarch64-apple-darwin" ;;
      *)      echo "Architecture non supportée: ${ARCH}"; exit 1 ;;
    esac
    ;;
  Linux)
    case "${ARCH}" in
      x86_64) TARGET="cube-x86_64-unknown-linux-musl" ;;
      *)      echo "Architecture non supportée: ${ARCH}"; exit 1 ;;
    esac
    ;;
  *)
    echo "OS non supporté: ${OS}"
    exit 1
    ;;
esac

# Fetch latest release tag
LATEST=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name"' | sed -E 's/.*"([^"]+)".*/\1/')

if [ -z "${LATEST}" ]; then
  echo "Impossible de déterminer la dernière version."
  exit 1
fi

URL="https://github.com/${REPO}/releases/download/${LATEST}/${TARGET}.tar.gz"

echo "Installation de cube ${LATEST} (${TARGET})..."
echo "  Téléchargement depuis ${URL}"

TMPDIR=$(mktemp -d)
trap 'rm -rf "${TMPDIR}"' EXIT

curl -fsSL "${URL}" | tar xz -C "${TMPDIR}"

if [ -w "${INSTALL_DIR}" ]; then
  mv "${TMPDIR}/cube" "${INSTALL_DIR}/cube"
else
  echo "  Écriture dans ${INSTALL_DIR} (sudo requis)"
  sudo mv "${TMPDIR}/cube" "${INSTALL_DIR}/cube"
fi

chmod +x "${INSTALL_DIR}/cube"

echo "cube ${LATEST} installé dans ${INSTALL_DIR}/cube"
