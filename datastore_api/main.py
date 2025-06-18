from fastapi import FastAPI

app = FastAPI()


@app.get("/health/alive")
async def alive():
    return "I'm alive!"


@app.get("/health/ready")
async def ready():
    return "I'm ready!"


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
