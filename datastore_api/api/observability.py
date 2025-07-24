from fastapi import APIRouter

router = APIRouter()


@router.get("/alive")
async def alive():
    return "I'm alive!"


@router.get("/ready")
async def ready():
    return "I'm ready!"
