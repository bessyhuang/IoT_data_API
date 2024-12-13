from fastapi import FastAPI, Depends

from app.internal import admin
from app.auth.rbac import get_current_active_user
from app.routers.iow import history_data, latest_data, latest_data_from_db, statistics_data
from app.routers.account import account


app = FastAPI()


app.include_router(
    latest_data.router,
    prefix="/iow/latest",
    tags=["詮釋資料 & 即時資料"],
)

app.include_router(
    latest_data_from_db.router,
    prefix="/iow/latest_from_db",
    tags=["詮釋資料 & 非即時資料 (from DB)"],
)

app.include_router(
    history_data.router,
    prefix="/iow/history",
    tags=["歷史資料"],
    dependencies=[Depends(get_current_active_user)],
)

app.include_router(
    statistics_data.router,
    prefix="/iow/statistics",
    tags=["統計資料"],
    dependencies=[Depends(get_current_active_user)],
)

app.include_router(
    account.router,
    prefix="/account",
    tags=["帳戶管理"]
)

# app.include_router(
#     admin.router,
#     prefix="/admin",
#     tags=["admin"],
#     dependencies=[Depends(get_token_header)],
# )


@app.get("/")
def root():
    return {"message": "Welcome to get data by yourself!"}
