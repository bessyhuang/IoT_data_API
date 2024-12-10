# auth/rbac.py
from fastapi import HTTPException, Depends
from database import users_db
from auth.jwt import decode_access_token

def get_current_user(token: str):
    payload = decode_access_token(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid token")
    username = payload.get("sub")
    user = users_db.get(username)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user

def has_role(required_roles: list):
    def role_checker(current_user: dict = Depends(get_current_user)):
        if current_user["role"] not in required_roles:
            raise HTTPException(status_code=403, detail="Access forbidden")
        return current_user
    return role_checker
