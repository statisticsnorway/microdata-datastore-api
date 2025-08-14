from fastapi import APIRouter

router = APIRouter()


@router.get("/alive")
async def alive() -> str:
    return "I'm alive!"


@router.get("/ready")
async def ready() -> str:
    return "I'm ready!"
