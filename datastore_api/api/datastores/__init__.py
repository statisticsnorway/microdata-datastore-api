from fastapi import APIRouter

from datastore_api.api.datastores import jobs

router = APIRouter()


router.include_router(jobs.router, prefix="/jobs")
