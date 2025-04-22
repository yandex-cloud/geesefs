FROM golang:1.22.10 as build

WORKDIR /workspace

COPY . .

RUN apt-get update && apt-get install -y fuse3 libfuse2 libfuse3-dev libfuse-dev bash-completion
RUN go build -o /usr/local/bin/geesefs -ldflags "-X main.Version=`git rev-parse HEAD`"

FROM golang:1.22.10

RUN apt-get update && apt-get install -y fuse3 libfuse2 libfuse3-dev libfuse-dev bash-completion
COPY --from=build /usr/local/bin/geesefs /usr/local/bin/geesefs
RUN chmod +x /usr/local/bin/geesefs

CMD ["/usr/local/bin/geesefs"]
