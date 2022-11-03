#!/bin/bash

set -e

# Start GeeseFS
[ -e /secrets/credentials ] && chmod 600 /secrets/credentials && chown root /secrets/credentials

mkdir -p /home/s3
useradd -d /home/s3 -s /bin/bash -p '*' $FTP_USER

/usr/bin/geesefs --shared-config /secrets/credentials --endpoint $S3_ENDPOINT \
    --uid $(id -u $FTP_USER) --gid $(id -g $FTP_USER) \
    -o allow_other --log-file stderr --dir-mode 0770 --file-mode 0660 $S3_BUCKET /home/s3

if [ "$SFTP" = "YES" ]; then
    # Regenerate SSH host keys if this is the first time the container is started
    INIT_FILE=/initialized
    if [ ! -f "$INIT_FILE" ]; then
        echo "Generating SSH host keys..."
        ssh-keygen -A
        echo "1" > $INIT_FILE
    fi
    # Start sshd
    cp /secrets/authorized_keys /authorized_keys
    chown $FTP_USER /authorized_keys
    chmod 600 /authorized_keys
    mkdir -p /run/sshd
    echo "Match User $FTP_USER" >> /etc/ssh/sshd_config
    echo "    AuthorizedKeysFile /authorized_keys" >> /etc/ssh/sshd_config
    /usr/sbin/sshd -D -e &
fi

if [ "$FTP" = "YES" ]; then
    if [ "$FTP_PASS" = "**Random**" ]; then
        export FTP_PASS=`cat /dev/urandom | tr -dc A-Z-a-z-0-9 | head -c${1:-16}`
        echo "FTP password: $FTP_PASS"
    fi
    echo "$FTP_USER:$FTP_PASS" | chpasswd

    # Set passive mode parameters:
    if [ "$FTP_PASV_ADDRESS" = "**IPv4**" ]; then
        export FTP_PASV_ADDRESS=$(/sbin/ip route|awk '/default/ { print $3 }')
    fi

    echo "pasv_address=${FTP_PASV_ADDRESS}" >> /etc/vsftpd.conf
    echo "pasv_max_port=${FTP_PASV_MAX_PORT}" >> /etc/vsftpd.conf
    echo "pasv_min_port=${FTP_PASV_MIN_PORT}" >> /etc/vsftpd.conf
    echo "pasv_addr_resolve=${FTP_PASV_ADDR_RESOLVE}" >> /etc/vsftpd.conf
    echo "pasv_enable=${FTP_PASV_ENABLE}" >> /etc/vsftpd.conf
    echo "pasv_promiscuous=${FTP_PASV_PROMISCUOUS}" >> /etc/vsftpd.conf
    echo "port_promiscuous=${FTP_PORT_PROMISCUOUS}" >> /etc/vsftpd.conf

    # Set SSL parameters
    if [ "$FTP_SSL_ENABLE" != "NO" ]; then
        if [ -f "$FTP_RSA_CERT_FILE" ]; then
            echo "rsa_cert_file=$FTP_RSA_CERT_FILE" >> /etc/vsftpd.conf
            echo "rsa_private_key_file=$FTP_RSA_PRIVATE_KEY_FILE" >> /etc/vsftpd.conf
            echo "ssl_enable=YES" >> /etc/vsftpd.conf
            if [ "$FTP_SSL_ENABLE" = "FORCE" ]; then
                echo "force_local_data_ssl=yes" >> /etc/vsftpd.conf
                echo "force_local_logins_ssl=yes" >> /etc/vsftpd.conf
            fi
        else
            echo "SSL certificate not provided - using plain FTP (insecure!)"
        fi
    else
        echo "SSL certificate not provided - using plain FTP (insecure!)"
    fi

    # Start the server
    #ln -s /dev/stderr /var/log/vsftpd.log
    mkdir -p /var/run/vsftpd/empty
    vsftpd /etc/vsftpd.conf &
fi

# Wait for any process to exit and die with its exitcode
wait -n
exit $?
