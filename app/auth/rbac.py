# auth/rbac.py
from fastapi import HTTPException, Depends, Security
from fastapi.security import SecurityScopes, OAuth2PasswordBearer

from typing import Annotated
from passlib.context import CryptContext

from app.auth.jwt import decode_access_token
from app.models.account_schemas import User, UserInDB
from app.config import get_mongodb_connection


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
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

# Get User metadata from DB
USER_dbClient = get_mongodb_connection('history')
USER_db = USER_dbClient.users


def get_user(username: str):
    user_data = list(USER_db['account'].find({'username': username}))[0]
    return UserInDB(**user_data) if user_data else None

def get_password_hash(password):
    return pwd_context.hash(password)

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

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
    """
    fn_security_scopes.scopes => 啟用該 function 必須有的權限
    db_user_scopes            => 該 user 擁有的權限 (DB 紀錄)
    """
    payload = decode_access_token(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid token")

    username = payload.get("sub")
    user = get_user(username)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    db_user_scopes = user.roles + user.functions
    has_permission = scope_checker(fn_security_scopes, db_user_scopes)
    print('===>', user.username, fn_security_scopes.scopes, db_user_scopes)
    if has_permission:
        return user

def get_current_active_user(
    current_user: Annotated[User, Security(get_current_user, scopes=["staff"])]
):
    if not current_user.is_active:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user

def scope_checker(required_scopes: list, user_scopes: list):
    has_permission = True
    for scope in required_scopes.scopes:
        if scope not in user_scopes:
            has_permission = False
            raise HTTPException(
                status_code=403, detail=f"Not enough permissions. Missing scope: {scope}",
            )
    return has_permission
