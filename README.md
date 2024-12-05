# IoT_data_API

## Small Applications
```bash
$ fastapi run main.py
```

## Bigger Applications
```bash
$ uvicorn app.main:app --reload --host 0.0.0.0 --port 5002
```

## Note:
- Monitoring Tool: 
    [Apitally](https://app.apitally.io/traffic/fastapi-1?period=24h)
    ```bash
    $ pip install apitally
    ```
    ```python
    # app/main.py
    from apitally.fastapi import ApitallyMiddleware

    app = FastAPI()

    # add
    app.add_middleware(
        ApitallyMiddleware,
        client_id="APITALLY_CLIENT_ID",
        env="prod",  # or "dev"
    )
    ```