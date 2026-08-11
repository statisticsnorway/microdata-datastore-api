import base64
import os
from typing import Final, Literal

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from pyarrow.parquet.encryption import CryptoFactory, KmsClient

from datastore_api.config import secrets

FOOTER_KEY_ID: Final[str] = "footer_key"
COLUMN_KEY_ID: Final[str] = "column_key"
type MasterKeyIdentifier = Literal[COLUMN_KEY_ID, FOOTER_KEY_ID]


def make_crypto_factory() -> CryptoFactory:
    return CryptoFactory(
        lambda _cfg: InMemoryKmsClient(
            footer_master_key_hex=secrets.parquet_footer_master_key,
            column_master_key_hex=secrets.parquet_column_master_key,
        )
    )


class InMemoryKmsClient(KmsClient):
    def __init__(
        self, footer_master_key_hex: str, column_master_key_hex: str
    ) -> None:
        super().__init__()
        self._footer_master_key = bytes.fromhex(footer_master_key_hex)
        self._column_master_key = bytes.fromhex(column_master_key_hex)

    def _resolve_master_key(
        self, master_key_identifier: MasterKeyIdentifier
    ) -> bytes:
        if master_key_identifier == FOOTER_KEY_ID:
            return self._footer_master_key
        if master_key_identifier == COLUMN_KEY_ID:
            return self._column_master_key
        raise ValueError(
            f"Unknown master key identifier: {master_key_identifier}"
        )

    def wrap_key(
        self, key_bytes: bytes, master_key_identifier: MasterKeyIdentifier
    ) -> bytes:
        """Encrypt a KEK before parquet stores it.

        In parquet encryption, column/footer data is encrypted with DEKs, and
        those keys are wrapped by a KEK. PyArrow calls this
        method during writes so that a KEK can be encrypted with the
        configured master key before being saved with the parquet output.

        We pick the master key by identifier, encrypt ``key_bytes`` with
        AES-GCM, and return a base64 payload parquet can persist.
        """
        master = self._resolve_master_key(master_key_identifier)
        nonce = os.urandom(12)
        ciphertext = AESGCM(master).encrypt(nonce, key_bytes, None)
        return base64.b64encode(nonce + ciphertext)

    def unwrap_key(
        self,
        wrapped_key: bytes,
        master_key_identifier: MasterKeyIdentifier,
    ) -> bytes:
        """Decrypt a KEK that parquet previously stored.

        PyArrow calls this while reading encrypted parquet files. We decode the
        stored payload, decrypt it with the selected master key, and return the
        plaintext KEK so parquet can continue unwrapping/decrypting the
        underlying encrypted data.
        """
        master = self._resolve_master_key(master_key_identifier)
        raw = base64.b64decode(wrapped_key)
        nonce, ciphertext = raw[:12], raw[12:]
        return AESGCM(master).decrypt(nonce, ciphertext, None)
