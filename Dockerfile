# 
FROM python:3.9

# 
WORKDIR /code

# 
COPY ./requirements.txt /code/requirements.txt

# 
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# 
COPY ./app /code/app
COPY ./app/.env /code/.env
COPY ./STATION_UUIDs /code/STATION_UUIDs
EXPOSE 8000

# 
CMD ["fastapi", "run", "/code/app/main.py", "--port", "8000"]
