#!/usr/bin/env bash

set -eo pipefail

YQ_VERSION=4.40.3
SCRIPTS_DIR=$(dirname "${BASH_SOURCE[0]}")
BIN_DIR=${SCRIPTS_DIR}/bin
YQ_INSTALL_PATH=${BIN_DIR}/yq-${YQ_VERSION}

install_yq() {
  local version=$1
  local binary_path=$2
  local platform
  local download_url

  platform="$(uname | tr '[:upper:]' '[:lower:]')_$(get_arch)"
  download_url="$(get_download_url "${version}" "${platform}")"

  curl -sfL "${download_url}" -o "${binary_path}"
  chmod +x "${binary_path}"
}

get_arch() {
  ARCH=$(uname -m)
  case $ARCH in
    armv*) ARCH="arm";;
    aarch64) ARCH="arm64";;
    x86) ARCH="386";;
    x86_64) ARCH="amd64";;
    i686) ARCH="386";;
    i386) ARCH="386";;
  esac
  echo "$ARCH"
}

get_download_url() {
  local version="$1"
  local platform="$2"
  local filename="yq_${platform}"
  echo "https://github.com/mikefarah/yq/releases/download/v${version}/${filename}"
}

if ! [ -x "$(command -v ${YQ_INSTALL_PATH})" ]; then
  mkdir -p "${BIN_DIR}"
  rm -f ${BIN_DIR}/yq-*
  install_yq "${YQ_VERSION}" "${YQ_INSTALL_PATH}"
fi

exec ${YQ_INSTALL_PATH} "${@}"
