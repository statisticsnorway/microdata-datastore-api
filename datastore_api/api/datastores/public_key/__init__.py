import logging
from pathlib import Path

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from fastapi import APIRouter, Body, Depends, Response

from datastore_api.adapter.auth.dependencies import (
    authorize_api_key,
    authorize_data_administrator,
)
from datastore_api.api.common.dependencies import get_datastore_root_dir
from datastore_api.common.exceptions import (
    PublicKeyAlreadyExistsException,
    PublicKeyInvalidException,
    PublicKeyNotFoundException,
)

PUBLIC_KEY_FILE_NAME = "microdata_public_key.pem"

logger = logging.getLogger()
router = APIRouter()


@router.get("", dependencies=[Depends(authorize_data_administrator)])
def get_public_key(
    datastore_root_dir: Path = Depends(get_datastore_root_dir),
) -> Response:
    public_key_location = datastore_root_dir / "vault" / PUBLIC_KEY_FILE_NAME
    if not public_key_location.exists():
        raise PublicKeyNotFoundException(
            f"Public key not found at {public_key_location}"
        )

    try:
        with open(public_key_location, "rb") as key_file:
            public_key = serialization.load_pem_public_key(
                key_file.read(), backend=default_backend()
            )

        pem_bytes = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )

        return Response(content=pem_bytes, media_type="application/x-pem-file")
    except Exception as e:
        raise PublicKeyInvalidException(
            f"Public key is invalid at {public_key_location}"
        ) from e


@router.post("", dependencies=[Depends(authorize_api_key)])
def save_public_key(
    public_key_bytes: bytes = Body(..., media_type="application/x-pem-file"),
    datastore_root_dir: Path = Depends(get_datastore_root_dir),
) -> None:
    public_key_location = datastore_root_dir / "vault" / PUBLIC_KEY_FILE_NAME

    if public_key_location.exists():
        raise PublicKeyAlreadyExistsException(
            f"Public key already exists at {public_key_location}"
        )

    try:
        # just validate
        serialization.load_pem_public_key(
            public_key_bytes, backend=default_backend()
        )
    except Exception as e:
        raise PublicKeyInvalidException("Public key is invalid") from e

    public_key_location.write_bytes(public_key_bytes)
    logger.info(f"Saved public key at {public_key_location}")
