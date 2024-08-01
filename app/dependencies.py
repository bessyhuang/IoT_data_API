from typing import Annotated

from fastapi import Header, HTTPException

from decouple import Config, RepositoryEnv
import os


# Load environment variables
base_dir = os.getcwd()
ENV = Config(RepositoryEnv(base_dir + '/.env'))


async def get_token_header(x_token: Annotated[str, Header()]):
    if x_token != ENV.get("X_TOKEN"):
        raise HTTPException(status_code=400, detail="X-Token header invalid")


async def get_query_token(token: str):
    if token != ENV.get("TOKEN"):
        raise HTTPException(status_code=400, detail="Invalid Token")
