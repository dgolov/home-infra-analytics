#!/bin/sh
set -e

if [ ! -f /etc/clickhouse-server/config.d/cluster.xml ]; then
    echo "Generating cluster.xml from template..."
    envsubst < /etc/clickhouse-server/config.d/cluster.xml.template \
        > /etc/clickhouse-server/config.d/cluster.xml
    echo "=== GENERATED cluster.xml ==="
    cat /etc/clickhouse-server/config.d/cluster.xml
fi

if [ -f /etc/clickhouse-server/users.d/infra_user.xml.template ] && \
   [ ! -f /etc/clickhouse-server/users.d/infra_user.xml ]; then
    echo "Generating infra_user.xml from template..."
    envsubst < /etc/clickhouse-server/users.d/infra_user.xml.template \
        > /etc/clickhouse-server/users.d/infra_user.xml
    echo "=== GENERATED infra_user.xml ==="
    cat /etc/clickhouse-server/users.d/infra_user.xml
fi


if [ -f /etc/clickhouse-server/config.d/listen_hosts.xml.template ] && \
   [ ! -f /etc/clickhouse-server/config.d/listen_hosts.xml ]; then
    echo "Generating listen_hosts.xml from template..."
    envsubst < /etc/clickhouse-server/config.d/listen_hosts.xml.template \
        > /etc/clickhouse-server/config.d/listen_hosts.xml
    echo "=== GENERATED listen_hosts.xml ==="
    cat /etc/clickhouse-server/config.d/listen_hosts.xml
fi

exec /entrypoint.sh
