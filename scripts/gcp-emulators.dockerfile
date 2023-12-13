FROM node:20.10.0-slim
ARG FIREBASE_CLI_VERSION
RUN export DEBIAN_FRONTEND=noninteractive && \
    apt-get update && apt-get install -y openjdk-17-jre && \
    apt-get clean -yq
RUN npm install -g firebase-tools@${FIREBASE_CLI_VERSION} && \
    firebase setup:emulators:ui && \
    firebase setup:emulators:firestore && \
    firebase setup:emulators:storage && \
    firebase setup:emulators:pubsub
ADD firebase/ /app
WORKDIR /app
ENTRYPOINT ["firebase", "--project", "chainstorage-local", "emulators:start", "--only"]
CMD ["firestore,pubsub,storage"]
