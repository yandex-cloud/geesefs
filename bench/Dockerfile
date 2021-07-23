FROM ubuntu:18.04
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get -y install --no-install-recommends \
            # s3fs dependencies \
            automake autotools-dev fuse g++ git libcurl4-gnutls-dev libfuse-dev \
            libssl-dev libxml2-dev make pkg-config \
            # for running goofys benchmark \
            curl python-setuptools python-pip gnuplot-nox imagemagick awscli \
            # finally, clean up to make image smaller \
            && apt-get clean
# goofys graph generation
RUN pip install numpy

WORKDIR /tmp

ENV PATH=$PATH:/usr/local/go/bin
ARG GOVER=1.12.6
RUN curl -O https://storage.googleapis.com/golang/go${GOVER}.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go${GOVER}.linux-amd64.tar.gz && \
    rm go${GOVER}.linux-amd64.tar.gz

RUN git clone --depth 1 https://github.com/s3fs-fuse/s3fs-fuse.git && \
    cd s3fs-fuse && ./autogen.sh && ./configure && make -j8 > /dev/null && make install && \
    cd .. && rm -Rf s3fs-fuse

# ideally I want to clear out all the go deps too but there's no
# way to do that with ADD
ENV PATH=$PATH:/root/go/bin
ADD . /root/go/src/github.com/yandex-cloud/geesefs
WORKDIR /root/go/src/github.com/yandex-cloud/geesefs
RUN go get . && make install

ENTRYPOINT ["/root/go/src/github.com/yandex-cloud/geesefs/bench/run_bench.sh"]
