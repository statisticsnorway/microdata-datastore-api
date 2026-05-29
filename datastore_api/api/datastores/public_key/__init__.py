import logging
from pathlib import Path

from fastapi import APIRouter, Body, Depends, Response
from microdata_tools import PublicKey

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

logger = logging.getLogger()
router = APIRouter()


@router.get("", dependencies=[Depends(authorize_data_administrator)])
def get_public_key(
    datastore_root_dir: Path = Depends(get_datastore_root_dir),
) -> Response:
    public_key_path = datastore_root_dir / "vault" / PublicKey.FILENAME
    if not public_key_path.exists():
        raise PublicKeyNotFoundException(
            f"Public key not found at {public_key_path}"
        )

    try:
        public_key = PublicKey.load_from_file(public_key_path)
        pem_bytes = public_key.serialize()
        return Response(content=pem_bytes, media_type="application/x-pem-file")
    except Exception as e:
        raise PublicKeyInvalidException(
            f"Public key is invalid at {public_key_path}"
        ) from e


@router.post("", dependencies=[Depends(authorize_api_key)])
def save_public_key(
    public_key_bytes: bytes = Body(..., media_type="application/x-pem-file"),
    datastore_root_dir: Path = Depends(get_datastore_root_dir),
) -> None:
    public_key_dir = datastore_root_dir / "vault"
    public_key_path = public_key_dir / PublicKey.FILENAME
    if public_key_path.exists():
        raise PublicKeyAlreadyExistsException(
            f"Public key already exists at {public_key_path}"
        )
    try:
        public_key = PublicKey.from_pem(public_key_bytes)
    except Exception as e:
        raise PublicKeyInvalidException("Public key is invalid") from e

    public_key.write_to_file(public_key_dir)
    logger.info(f"Saved public key at {public_key_dir}")
