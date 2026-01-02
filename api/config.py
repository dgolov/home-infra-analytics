from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "FastAPI"
    debug: bool = False

    clickhouse_host: str
    clickhouse_db: str
    clickhouse_user: str
    clickhouse_password: str

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )


settings = Settings()
