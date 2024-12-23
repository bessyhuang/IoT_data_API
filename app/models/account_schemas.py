from pydantic import BaseModel
from typing import Optional, List
from enum import Enum


class Role(str, Enum):
    admin = "admin"
    staff = "staff"
    guest = "guest"

class Function(str, Enum):
    project = "project"
    device_list = "device_list"
    replacement = "replacement"
    map = "map"

class User(BaseModel):
    username: str
    hashed_password: str
    full_name: str
    email: str
    gender: Optional[str] = None
    is_active: bool
    roles: List[Role]
    functions: List[Function]

class UserInDB(User):
    hashed_password: str

class Token(BaseModel):
    access_token: str
    token_type: str
