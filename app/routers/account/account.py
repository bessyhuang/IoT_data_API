"""User API"""

from decouple import Config, RepositoryEnv
from typing import Annotated, List
from pymongo import MongoClient
from hashlib import sha256
import os

from fastapi import APIRouter, Body

from app.schemas import User


router = APIRouter()

# Load environment variables
base_dir = os.getcwd()
ENV = Config(RepositoryEnv(base_dir + '/.env'))

# Get User metadata from DB
USER_dbClient = MongoClient(ENV.get('HISTORY_DB_HOST_PORT'), username=ENV.get('HISTORY_DB_USER'), password=ENV.get('HISTORY_DB_PASSWORD'), authSource=ENV.get('HISTORY_DB_AUTH_SOURCE'))
USER_db = USER_dbClient.users


@router.post("/api/v1/create-user", response_model=User)
def insert_user(user: User):
    user.password = sha256(user.password.encode('utf-8')).hexdigest()
    inserted_user = USER_db['account'].insert_one(user.dict())
    return inserted_user

@router.get("/api/v1/read-all-users", response_model=List[User])
async def read_users():
    users = list(USER_db['account'].find())
    return users
