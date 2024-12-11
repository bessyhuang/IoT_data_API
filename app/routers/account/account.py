"""User API"""

from decouple import Config, RepositoryEnv
from datetime import timedelta
from typing import Annotated, List
from pymongo import MongoClient
from passlib.context import CryptContext
import os

from fastapi import APIRouter, Depends, HTTPException, Security
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm, SecurityScopes

from app.schemas import User, UserInDB, Token
from app.auth.jwt import create_access_token, decode_access_token

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
ACCESS_TOKEN_EXPIRE_MINUTES = 30
oauth2_scheme = OAuth2PasswordBearer(
    tokenUrl="/account/api/token",
    auto_error=False,
    scopes={
        "admin": "Admin access",
        "staff": "Staff access",
        "guest": "Guest access",
        "project": "func: project",
        "map": "func: map",
    }
)

router = APIRouter()

# Load environment variables
base_dir = os.getcwd()
ENV = Config(RepositoryEnv(base_dir + '/.env'))

# Get User metadata from DB
USER_dbClient = MongoClient(ENV.get('HISTORY_DB_HOST_PORT'), username=ENV.get('HISTORY_DB_USER'), password=ENV.get('HISTORY_DB_PASSWORD'), authSource=ENV.get('HISTORY_DB_AUTH_SOURCE'))
USER_db = USER_dbClient.users


def get_password_hash(password):
    return pwd_context.hash(password)

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_user(username: str):
    user_data = list(USER_db['account'].find({'username': username}))[0]
    return UserInDB(**user_data) if user_data else None

def authenticate_user(username: str, password: str):
    user = get_user(username)
    if not user:
        raise HTTPException(status_code=404, detail="Account not registered or incorrect account name")
    if not verify_password(password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Invalid password")
    return user

def get_current_user(
    fn_security_scopes: SecurityScopes,
    token: Annotated[str, Depends(oauth2_scheme)]
):
    payload = decode_access_token(token)
    username: str = payload.get("sub")

    # fn_security_scopes.scopes => 啟用該 function 必須有的權限
    # db_UserToken["scopes"]    => 該 user 擁有的權限
    db_UserToken = USER_db['account'].find_one({'username': username})
    db_UserToken_scopes = db_UserToken["roles"] + db_UserToken["functions"]
    for scope in fn_security_scopes.scopes:
        if scope not in db_UserToken_scopes:
            raise HTTPException(
                status_code=403, detail=f"Not enough permissions. Missing scope: {scope}",
            )
    user = get_user(username)
    return user

def get_current_active_user(
    current_user: Annotated[User, Security(get_current_user, scopes=["staff"])],
):
    if not current_user.is_active:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user

# Routes for OAuth2 and scopes
@router.post("/api/token")
def login_for_access_token(form_data: Annotated[OAuth2PasswordRequestForm, Depends()]) -> Token:
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=401, detail="Incorrect username or password")

    access_token = create_access_token(
        data={"sub": user.username, "scopes": form_data.scopes},
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
    )
    return Token(access_token=access_token, token_type="bearer")

@router.get("/api/v1/read-all-users", response_model=List[User])
async def read_users(current_user: Annotated[User, Depends(get_current_active_user)]):
    # User must have ["staff"] in scopes & is_active=True to access function
    users = list(USER_db['account'].find())
    return users

@router.get("/api/v1/map")
async def view_map(current_user: Annotated[User, Security(get_current_active_user, scopes=["map"])]):
    # User must have ["staff", "map"] in scopes & is_active=True to access function
    return [{"owner": current_user.username, "role": current_user.roles, "function": current_user.functions, 'map': "xxx"}]

@router.get("/api/v1/status/")
async def read_system_status(current_user: Annotated[User, Depends(get_current_user)]):
    # User is active or inactive can access function
    return {"status": "ok"}

@router.post("/api/v1/create-user", response_model=User)
def insert_user(current_user: Annotated[User, Security(get_current_active_user, scopes=["admin"])], user: User):
    user.hashed_password = get_password_hash(user.hashed_password)
    USER_db['account'].insert_one(user.dict())
    return user
