#!/usr/bin/env bash

set -eo pipefail

JQ_VERSION=1.7.1
SCRIPTS_DIR=$(dirname "${BASH_SOURCE[0]}")
BIN_DIR=${SCRIPTS_DIR}/bin
JQ_INSTALL_PATH=${BIN_DIR}/jq-${JQ_VERSION}

install_jq() {
  local version=$1
  local binary_path=$2
  local platform
  local download_url

  platform="$(get_os)-$(get_arch)"
  download_url="$(get_download_url "${version}" "${platform}")"

  curl -sfL "${download_url}" -o "${binary_path}"
  chmod +x "${binary_path}"
}

get_arch() {
  ARCH=$(uname -m)
  case $ARCH in
    armv*) ARCH="arm";;
    aarch64) ARCH="arm64";;
    x86) ARCH="i386";;
    x86_64) ARCH="amd64";;
    i686) ARCH="i386";;
    i386) ARCH="i386";;
  esac
  echo "$ARCH"
}

get_os() {
  OS=$(uname -o)
  case $OS in
    *Linux) OS="linux";;
    Darwin) OS="macos";;
    Msys) ARCH="windows";;
  esac
  echo "$OS"
}

get_download_url() {
  local version="$1"
  local platform="$2"
  local filename="jq-${platform}"
  echo https://github.com/jqlang/jq/releases/download/jq-${version}/${filename}
}

if ! [ -x "$(command -v ${JQ_INSTALL_PATH})" ]; then
  mkdir -p "${BIN_DIR}"
  rm -f ${BIN_DIR}/jq-*
  install_jq "${JQ_VERSION}" "${JQ_INSTALL_PATH}"
fi

exec ${JQ_INSTALL_PATH} "${@}"
