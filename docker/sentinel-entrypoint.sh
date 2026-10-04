#!/bin/sh
set -eu

sentinel_conf=/data/sentinel.conf

# Redis Sentinel rewrites its configuration while discovering peers and
# completing failovers, so keep this file on a writable per-sentinel volume.
if [ ! -f "$sentinel_conf" ]; then
  cat > "$sentinel_conf" <<EOF
port ${SENTINEL_PORT}
bind 0.0.0.0
protected-mode no
dir /data
sentinel resolve-hostnames yes
sentinel announce-hostnames yes
sentinel monitor mymaster 172.30.80.10 6379 2
sentinel down-after-milliseconds mymaster 5000
sentinel failover-timeout mymaster 15000
sentinel parallel-syncs mymaster 1
EOF
fi

exec redis-server "$sentinel_conf" --sentinel
