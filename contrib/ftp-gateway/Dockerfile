FROM debian:bullseye

# Install GeeseFS
RUN echo "APT::Install-Recommends false;" >> /etc/apt/apt.conf
RUN apt-get update && apt-get -y install fuse && apt-get clean && rm -rf /var/lib/apt/lists/*
ADD https://github.com/yandex-cloud/geesefs/releases/latest/download/geesefs-linux-amd64 /usr/bin/geesefs
RUN chmod 755 /usr/bin/geesefs

# Install openssh & vsftpd
RUN apt-get update && \
    apt-get -y install openssh-server vsftpd ca-certificates iproute2 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY sshd_config /etc/ssh/
COPY vsftpd.conf /etc/
COPY start.sh /scripts/start.sh

ENV S3_ENDPOINT https://storage.yandexcloud.net

ENV SFTP YES

ENV FTP NO

ENV FTP_USER s3
ENV FTP_PASS **Random**
ENV FTP_PASV_ENABLE YES
ENV FTP_PASV_ADDRESS **IPv4**
ENV FTP_PASV_ADDR_RESOLVE YES
ENV FTP_PASV_MIN_PORT 21100
ENV FTP_PASV_MAX_PORT 21100
ENV FTP_PASV_PROMISCUOUS NO
ENV FTP_PORT_PROMISCUOUS NO
ENV FTP_SSL_ENABLE NO
ENV FTP_RSA_CERT_FILE /secrets/ftp.pem
ENV FTP_RSA_PRIVATE_KEY_FILE /secrets/ftp.key

EXPOSE 22 23

CMD ["/scripts/start.sh"]
