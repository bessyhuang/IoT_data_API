from pydantic_settings import BaseSettings, SettingsConfigDict
from pymongo import MongoClient
from functools import lru_cache


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="allow", env_file=".env")
    app_name: str = "MyFastAPI App"

    # IOW_PAYLOAD
    API_GRANT_TYPE: str
    API_CLIENT_ID: str
    API_CLIENT_SECRET: str

    # AIOT
    AIOT_username: str
    AIOT_password: str

    # PRIMARY_DB (Yanina)
    DB_HOST_PORT: str
    DB_USER: str
    DB_PASSWORD: str
    DB_AUTH_SOURCE: str

    # HISTORY_DB (Testing)
    HISTORY_DB_HOST_PORT: str
    HISTORY_DB_USER: str
    HISTORY_DB_PASSWORD: str
    HISTORY_DB_AUTH_SOURCE: str

    # JWT
    SECRET_KEY: str
    ALGORITHM: str
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30


settings = Settings()


@lru_cache()
def get_mongodb_connection(tag: str):
    if tag == 'history':
        dbClient = MongoClient(
            settings.HISTORY_DB_HOST_PORT,
            username=settings.HISTORY_DB_USER,
            password=settings.HISTORY_DB_PASSWORD,
            authSource=settings.HISTORY_DB_AUTH_SOURCE
        )
    elif tag == 'metadata':
        dbClient = MongoClient(
            settings.DB_HOST_PORT,
            username=settings.DB_USER,
            password=settings.DB_PASSWORD,
            authSource=settings.DB_AUTH_SOURCE
        )
    return dbClient
