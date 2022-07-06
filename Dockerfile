# Image to build geesefs for ubuntu 18
# USE instructions
# chcon -Rt svirt_sandbox_file_t .
# sudo docker run -it -v $PWD:/work gb bash
# (inside container) go build

FROM ubuntu:18.04

RUN apt-get update && apt-get install -y git wget
ADD . /work
WORKDIR /work
RUN wget https://dl.google.com/go/go1.16.4.linux-amd64.tar.gz && tar -xvf go1.16.4.linux-amd64.tar.gz && mv go /usr/local
RUN ln -s /usr/local/go/bin/go /usr/bin/go
