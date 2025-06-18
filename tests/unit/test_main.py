import pytest
from datastore_api import main


@pytest.mark.asyncio
async def test_alive():
    assert await main.alive() == "I'm alive!"


@pytest.mark.asyncio
async def test_ready():
    assert await main.ready() == "I'm ready!"
