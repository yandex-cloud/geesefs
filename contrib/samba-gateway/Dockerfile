# Dockerfile for GeeseFS Samba Gateway
#
# Provides SMB/Samba access to S3 buckets mounted via GeeseFS
#
# Build:
#   docker build -t samba-s3 .
#
# Run:
#   docker run -d --name samba-s3 --privileged \
#     -p 9445:445 -p 9139:139 \
#     -e S3_ACCESS_KEY_ID=KEY-ID \
#     -e S3_SECRET_ACCESS_KEY=SECRET-KEY \
#     -e S3_BUCKET=my-bucket \
#     -e S3_ENDPOINT=https://storage.yandexcloud.net \
#     -e SAMBA_PASSWORD=secret \
#     samba-s3

FROM ubuntu:24.04

ARG DEBIAN_FRONTEND=noninteractive

# Install geesefs, FUSE, SSL certificates, and Samba
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
        fuse3 \
        samba \
        samba-common-bin \
        winbind \
        krb5-user \
    && curl -fsSL --retry 3 --retry-delay 5 -o /usr/local/bin/geesefs \
        "https://github.com/yandex-cloud/geesefs/releases/latest/download/geesefs-linux-amd64" \
    && chmod +x /usr/local/bin/geesefs \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create mount point and secrets directory
RUN mkdir -p /data /secrets

# Copy configuration files
COPY smb.conf /etc/samba/smb.conf
COPY start.sh /start.sh
RUN chmod +x /start.sh

# Expose Samba port
EXPOSE 445 139

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD mountpoint -q /data && pgrep -x smbd > /dev/null || exit 1

ENTRYPOINT ["/start.sh"]
