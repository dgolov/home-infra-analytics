from clickhouse_driver import Client

from config import settings


def get_clickhouse_client():
    client = Client(
        host=settings.clickhouse_host,
        database=settings.clickhouse_db,
        user=settings.clickhouse_user,
        password=settings.clickhouse_password,
    )
    try:
        yield client
    finally:
        client.disconnect()
