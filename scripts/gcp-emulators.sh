#!/usr/bin/env bash
set -euo pipefail
FIREBASE_CLI_VERSION=12.9.1

SCRIPTS_DIR=$(dirname "${BASH_SOURCE[0]}")
IMAGE_NAME=firebase-emulators:v${FIREBASE_CLI_VERSION}
CONTAINER_NAME=chainstorage-firebase-emulators

alias jq="$SCRIPTS_DIR/jq.sh"

prepare_image() {
  if ! docker image inspect ${IMAGE_NAME} >/dev/null 2>&1; then
    pushd $SCRIPTS_DIR &> /dev/null
    docker build -t ${IMAGE_NAME} -f gcp-emulators.dockerfile --build-arg FIREBASE_CLI_VERSION=$FIREBASE_CLI_VERSION .
    popd &> /dev/null
  fi
}

print_env() {
  if ! docker container inspect ${CONTAINER_NAME} >/dev/null 2>&1; then
    >&2 echo "Emulators not running"
    exit 1
  fi
  HOST=$(docker container inspect ${CONTAINER_NAME} | jq -r '.[0].NetworkSettings.IPAddress')
  CONFIG=$(curl -s http://$HOST:4000/api/config)
  STORAGE_PORT=$(echo $CONFIG | jq -r '.storage.port')
  FIRESTORE_PORT=$(echo $CONFIG | jq -r '.firestore.port')
  PUBSUB_PORT=$(echo $CONFIG | jq -r '.pubsub.port')
  PROJECT_ID=$(echo $CONFIG | jq -r '.projectId')
  echo "export CHAINSTORAGE_GCP_PROJECT=${PROJECT_ID}"
  echo "export FIRESTORE_EMULATOR_HOST=${HOST}:${FIRESTORE_PORT}"
  echo "export PUBSUB_EMULATOR_HOST=${HOST}:${PUBSUB_PORT}"
  echo "export STORAGE_EMULATOR_HOST=${HOST}:${STORAGE_PORT}"
}

main() {
  local cmd=${1:-""}
  case $cmd in
  build)
    if docker container inspect ${CONTAINER_NAME} >/dev/null 2>&1; then
      >&2 echo "Emulators running, please stop them first"
      exit 1
    fi
    if docker image inspect ${IMAGE_NAME} >/dev/null 2>&1; then
      docker image rm ${IMAGE_NAME}
    fi
    prepare_image
    ;;
  run)
    prepare_image
    if ! docker container inspect ${CONTAINER_NAME} >/dev/null 2>&1; then
      docker run --name ${CONTAINER_NAME} --rm $IMAGE_NAME
    else
      echo "Emulators already running"
    fi
    ;;
  up)
    prepare_image
    if ! docker container inspect ${CONTAINER_NAME} >/dev/null 2>&1; then
      docker run --name ${CONTAINER_NAME} -d $IMAGE_NAME
    else
      docker start ${CONTAINER_NAME}
    fi
    echo "Waiting for gcp emulators to be ready..."
    sleep 1
    HOST=$(docker container inspect ${CONTAINER_NAME} | jq -r '.[0].NetworkSettings.IPAddress')
    for i in $(seq 1 999); do
      if [ ! -z "$(curl -s http://${HOST}:4000/api/config | jq -r '.projectId')" ]; then
        break
      fi
      sleep 0.3
      if [ $(($i % 30)) -eq 0 ]; then
        echo .
      fi 
    done
    echo "gcp emulators ready"
    ;;
  down)
    if docker container inspect ${CONTAINER_NAME} >/dev/null 2>&1; then
      docker stop ${CONTAINER_NAME} &> /dev/null
      docker rm -f ${CONTAINER_NAME} &> /dev/null
      echo "Emulators stopped"
    else
      echo "Emulators not running"
    fi
    ;;
  logs)
    docker logs -f --tail=1000 ${CONTAINER_NAME}
    ;;
  reset)
    prepare_image
    docker rm -f ${CONTAINER_NAME} &> /dev/null
    main up
    ;;
  env)
    print_env
    ;;
  exec)
    shift
    if ! docker container inspect ${CONTAINER_NAME} >/dev/null 2>&1; then
      main up
      trap "{ main down; }" EXIT
    fi
    eval $(main env)
    ( exec "${@}" )&
    wait
    ;;
  *)
    echo "Usage: $0 <build|run|up|down|logs|reset|env|exec>"
    exit 1
    ;;
  esac
}

main "${@}"
