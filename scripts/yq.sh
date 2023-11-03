#!/usr/bin/env bash

set -eo pipefail

YQ_VERSION=4.35.2
SCRIPTS_DIR=$(dirname "${BASH_SOURCE[0]}")
YQ_INSTALL_PATH=${SCRIPTS_DIR}/binary/yq-${YQ_VERSION}

install_yq() {
  local version=$1
  local binary_path=$2
  local platform
  local download_url

  platform="$(uname | tr '[:upper:]' '[:lower:]')_$(getArch)"
  download_url="$(get_download_url "${version}" "${platform}")"

  curl -sfL "${download_url}" -o "${binary_path}"
  chmod +x "${binary_path}"
}

getArch() {
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

get_filename() {
  local platform="$1"
  echo "yq_${platform}"
}

get_download_url() {
  local version="$1"
  local platform="$2"
  local filename
  local major
  local minor
  local patch

  major="$(echo "${version}" | cut -f1 -d.)"
  minor="$(echo "${version}" | cut -f2 -d.)"
  patch="$(echo "${version}" | cut -f3 -d.)"

  if [[ "${platform}" == "darwin_arm64" && ("${major}" -lt 4 || ("${major}" -eq 4 && "${minor}" -lt 9) || ("${major}" -eq 4 && "${minor}" -eq 9 && "${patch}" -lt 6)) ]] ; then
    # arm64 builds are introduced from 4.9.6 onwards
    platform="darwin_amd64"
  fi

  filename="$(get_filename "${platform}")"

  if [[ "${major}" -gt 4 || ("${major}" -eq 4 && "${minor}" -gt 0) ]] ; then
    version="v${version}"
  fi

  echo "https://github.com/mikefarah/yq/releases/download/${version}/${filename}"
}

if ! [ -x "$(command -v ${YQ_INSTALL_PATH})" ]; then
  mkdir -p "${SCRIPTS_DIR}/binary"
  install_yq "${YQ_VERSION}" "${YQ_INSTALL_PATH}"
fi

exec ${YQ_INSTALL_PATH} "$1" "$2" "$3"
