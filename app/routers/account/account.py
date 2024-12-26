"""User API"""

from decouple import Config, RepositoryEnv
from datetime import timedelta
from typing import Annotated, List
from pymongo import MongoClient
import os

from fastapi import APIRouter, Depends, HTTPException, Security
from fastapi.security import OAuth2PasswordRequestForm

from app.schemas import User, Token
from app.auth.jwt import create_access_token
from app.auth.rbac import authenticate_user, get_current_active_user, get_current_user, get_password_hash


router = APIRouter()

# Load environment variables
base_dir = os.getcwd()
ENV = Config(RepositoryEnv(base_dir + '/.env'))
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Get User metadata from DB
USER_dbClient = MongoClient(
    ENV.get('HISTORY_DB_HOST_PORT'),
    username=ENV.get('HISTORY_DB_USER'),
    password=ENV.get('HISTORY_DB_PASSWORD'),
    authSource=ENV.get('HISTORY_DB_AUTH_SOURCE')
)
USER_db = USER_dbClient.users


# Routes for OAuth2 and scopes
@router.post("/api/token")
def login_for_access_token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()]
) -> Token:
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=401, detail="Incorrect username or password")

    access_token = create_access_token(
        data={"sub": user.username, "scopes": form_data.scopes},
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
    )
    return Token(access_token=access_token, token_type="bearer")


@router.get("/api/v1/read-all-users", response_model=List[User])
async def read_users(
    current_user: Annotated[User, Depends(get_current_active_user)]
):
    # User must have ["staff"] in scopes & is_active=True to access function
    users = list(USER_db['account'].find())
    return users


@router.get("/api/v1/read-user/{username}", response_model=User)
async def read_user(
    current_user: Annotated[User, Depends(get_current_active_user)],
    username: str
):
    user = USER_db['account'].find_one({'username': username})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


@router.get("/api/v1/map")
async def view_map(
    current_user: Annotated[User, Security(get_current_active_user, scopes=["map"])]
):
    # User must have ["staff", "map"] in scopes & is_active=True to access function
    return [{"owner": current_user.username, "role": current_user.roles, "function": current_user.functions, 'map': "xxx"}]


@router.get("/api/v1/status/")
async def read_system_status(
    current_user: Annotated[User, Depends(get_current_user)]
):
    # User is active or inactive can access function
    return {"status": "ok"}


@router.post("/api/v1/create-user", response_model=User)
def insert_user(
    current_user: Annotated[User, Security(get_current_active_user, scopes=["admin"])],
    user: User
):
    user.hashed_password = get_password_hash(user.hashed_password)
    USER_db['account'].insert_one(user.dict())
    return user
