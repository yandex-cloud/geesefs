#!/bin/bash

set -e

die() {
    echo "ERROR: $*" >&2
    exit 1
}

require_var() {
    local name="$1"
    local value="${!name}"
    if [ -z "$value" ]; then
        die "$name is required"
    fi
}

SAMBA_USER=${SAMBA_USER:-geesefs}
SAMBA_GROUP=${SAMBA_GROUP:-Domain Users}

require_var S3_BUCKET
require_var S3_ENDPOINT
require_var S3_ACCESS_KEY_ID
require_var S3_SECRET_ACCESS_KEY

if [ "$SAMBA_AD_MODE" = "YES" ]; then
    require_var SAMBA_REALM
    require_var SAMBA_WORKGROUP
    require_var SAMBA_KDC
    require_var SAMBA_AD_PASSWORD
else
    require_var SAMBA_PASSWORD
fi

cleanup() {
    echo "Shutting down..."
    smbd --shutdown 2>/dev/null || true
    nmbd --shutdown 2>/dev/null || true
    if [ "$SAMBA_AD_MODE" = "YES" ]; then
        killall winbindd 2>/dev/null || true
    fi
    pkill geesefs 2>/dev/null || true
    fusermount3 -u /data 2>/dev/null || umount /data 2>/dev/null || true
}

trap cleanup SIGTERM SIGINT

mkdir -p /home/"$SAMBA_USER"
if ! id "$SAMBA_USER" &>/dev/null; then
    useradd -d "/home/$SAMBA_USER" -s /bin/bash -p '*' "$SAMBA_USER"
fi

export AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$S3_SECRET_ACCESS_KEY"

/usr/local/bin/geesefs \
    --endpoint "$S3_ENDPOINT" \
    --uid "$(id -u "$SAMBA_USER")" --gid "$(id -g "$SAMBA_USER")" \
    -o allow_other --log-file stderr --dir-mode 0770 --file-mode 0660 \
    "$S3_BUCKET" /data &

GEESEFS_PID=$!

for _ in $(seq 1 30); do
    if mountpoint -q /data; then
        break
    fi
    if ! pgrep -x geesefs > /dev/null; then
        die "GeeseFS exited before mounting /data"
    fi
    sleep 1
done

mountpoint -q /data || die "GeeseFS failed to mount /data within 30 seconds"

write_standalone_share() {
    cat >> /etc/samba/smb.conf <<EOF

[geesefs]
   path = /data
   browseable = yes
   read only = no
   guest ok = no
   valid users = $SAMBA_USER
   create mask = 0660
   directory mask = 0770
   force user = $SAMBA_USER
   force group = $SAMBA_USER
EOF
}

write_ad_smb_conf() {
    cat > /etc/samba/smb.conf <<EOF
[global]
   workgroup = $SAMBA_WORKGROUP
   realm = $SAMBA_REALM
   security = ads
   idmap config * : backend = tdb
   idmap config * : range = 3000-7999
   idmap config $SAMBA_WORKGROUP : backend = ad
   idmap config $SAMBA_WORKGROUP : range = 10000-999999
   winbind use default domain = yes
   winbind offline logon = false
   log file = /var/log/samba/log.%m
   max log size = 1000
   logging = file
   server min protocol = SMB2
   server max protocol = SMB3
   unix extensions = no

[geesefs]
   path = /data
   browseable = yes
   read only = no
   guest ok = no
   valid users = @$SAMBA_GROUP
   create mask = 0660
   directory mask = 0770
   force user = $SAMBA_USER
   force group = $SAMBA_USER
EOF
}

if [ "$SAMBA_AD_MODE" = "YES" ]; then
    echo "Configuring Active Directory mode..."

    SAMBA_LOWER_REALM=$(echo "$SAMBA_REALM" | tr '[:upper:]' '[:lower:]')

    cat > /etc/krb5.conf <<EOF
[libdefaults]
    default_realm = $SAMBA_REALM
    dns_lookup_realm = false
    dns_lookup_kdc = false

[realms]
    $SAMBA_REALM = {
        kdc = $SAMBA_KDC
        admin_server = $SAMBA_KDC
    }

[domain_realm]
    .$SAMBA_LOWER_REALM = $SAMBA_REALM
    $SAMBA_LOWER_REALM = $SAMBA_REALM
EOF

    write_ad_smb_conf

    if ! net ads join -U "administrator%${SAMBA_AD_PASSWORD}"; then
        die "Failed to join Active Directory domain $SAMBA_REALM"
    fi

    mkdir -p /var/run/samba
    winbindd --daemon --no-process-group
    sleep 2

    if ! pgrep -x winbindd > /dev/null; then
        die "winbindd failed to start"
    fi
else
    if ! echo -e "$SAMBA_PASSWORD\n$SAMBA_PASSWORD" | smbpasswd -a -s "$SAMBA_USER"; then
        die "Failed to set Samba password for user $SAMBA_USER"
    fi
    write_standalone_share
fi

mkdir -p /var/log/samba
echo "Starting Samba service..."

nmbd --daemon --no-process-group
sleep 2

smbd --daemon --no-process-group
sleep 2

pgrep -x smbd > /dev/null || die "smbd failed to start"

echo "All services started. GeeseFS PID: $GEESEFS_PID"

while true; do
    if ! pgrep -x geesefs > /dev/null; then
        die "GeeseFS process exited unexpectedly"
    fi
    if ! pgrep -x smbd > /dev/null; then
        die "smbd process exited unexpectedly"
    fi
    sleep 5
done
