import logging
from pathlib import Path

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from fastapi import APIRouter, Body, Depends, Header, Response

from datastore_api.adapter import auth
from datastore_api.api.common.dependencies import get_datastore_root_dir
from datastore_api.common.exceptions import (
    PublicKeyAlreadyExistsException,
    PublicKeyInvalidException,
    PublicKeyNotFoundException,
)

PUBLIC_KEY_FILE_NAME = "microdata_public_key.pem"

logger = logging.getLogger()
router = APIRouter()


@router.get("")
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


@router.post("")
def create_public_key(
    api_key: str = Header("X-API-KEY", alias="x-api-key"),
    public_key_bytes: bytes = Body(..., media_type="application/x-pem-file"),
    datastore_root_dir: Path = Depends(get_datastore_root_dir),
    auth_client: auth.AuthClient = Depends(auth.get_auth_client),
) -> None:
    auth_client.check_api_key(api_key)
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
    logger.info(f"Created public key at {public_key_location}")
