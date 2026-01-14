#!/bin/sh
set -e

generate_from_template() {
    local template="$1"
    local target="$2"
    local description="$3"

    if [ -f "$template" ] && [ ! -f "$target" ]; then
        echo "Generating $description from template..."
        envsubst < "$template" > "$target"
        echo "=== GENERATED $description ==="
        cat "$target"
    fi
}

generate_from_template "/etc/clickhouse-server/config.d/cluster.xml.template" \
                       "/etc/clickhouse-server/config.d/cluster.xml" \
                       "cluster.xml"

generate_from_template "/etc/clickhouse-server/users.d/infra_user.xml.template" \
                       "/etc/clickhouse-server/users.d/infra_user.xml" \
                       "infra_user.xml"

generate_from_template "/etc/clickhouse-server/config.d/listen_hosts.xml.template" \
                       "/etc/clickhouse-server/config.d/listen_hosts.xml" \
                       "listen_hosts.xml"

exec /entrypoint.sh
