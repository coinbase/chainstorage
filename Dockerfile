FROM golang:1.18 as build

ENV GO111MODULE on

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . ./
RUN make bin

FROM golang:1.18 as deploy

WORKDIR /app/

COPY --from=build /app/bin/* ./

CMD ["/app/server"]
