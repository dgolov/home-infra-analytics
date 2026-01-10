FROM clickhouse/clickhouse-server:25.12

RUN apt-get update && apt-get install -y gettext-base && rm -rf /var/lib/apt/lists/*

WORKDIR /etc/clickhouse-server

COPY init-config.sh /init-config.sh
RUN chmod +x /init-config.sh

EXPOSE 8123

ENTRYPOINT ["/init-config.sh"]
