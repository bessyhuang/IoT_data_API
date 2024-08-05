from fastapi import FastAPI, Depends

from .dependencies import get_query_token, get_token_header
from .internal import admin
from .routers.iow import history_data, latest_data, latest_data_from_db, statistics_data


app = FastAPI()
# app = FastAPI(dependencies=[Depends(get_query_token)])


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
    dependencies=[Depends(get_query_token)],
)

app.include_router(
    statistics_data.router,
    prefix="/iow/statistics",
    tags=["統計資料"],
    dependencies=[Depends(get_query_token)],
)


# app.include_router(
#     admin.router,
#     prefix="/admin",
#     tags=["admin"],
#     dependencies=[Depends(get_token_header)],
# )


@app.get("/")
async def root():
    return {"message": "Welcome to get data by yourself!"}