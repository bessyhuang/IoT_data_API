# 
FROM python:3.12-slim

# 
WORKDIR /code

# 
COPY ./requirements_uv.txt /code/requirements_uv.txt

# 
RUN pip install uv
RUN uv pip install --system --no-cache-dir -r /code/requirements_uv.txt

# 
COPY ./app /code/app
COPY ./app/.env /code/.env
COPY ./STATION_UUIDs /code/STATION_UUIDs
EXPOSE 8000
EXPOSE 27017

# 
CMD ["fastapi", "run", "/code/app/main.py", "--port", "8000"]
