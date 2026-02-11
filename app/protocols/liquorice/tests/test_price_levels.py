import asyncio
from typing import cast
from unittest.mock import MagicMock, patch

import pytest
from eth_typing import ChecksumAddress
from web3.main import to_checksum_address

from app.protocols.liquorice.price_levels import LiquoricePriceLevelPublisher
from app.protocols.liquorice.schemas import (
    PriceLevelLite,
    PriceLevelsMessage,
)


@pytest.mark.asyncio
async def test_price_levels_schema():
    """Verify PriceLevelsMessage serialization and validation."""
    level = PriceLevelLite(price="1", amount="1000.5")
    msg = PriceLevelsMessage(
        chainId=42161,
        baseToken=to_checksum_address("0xaf88d065e77c8cC2239327C5EDb3A432268e5831"),  # USDC
        quoteToken=to_checksum_address("0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9"),  # USDT
        levels=[level],
    )

    assert msg.chainId == 42161
    assert len(msg.levels) == 1
    assert msg.levels[0].price == "1"
    assert msg.levels[0].amount == "1000.5"

    # Test address validation
    with pytest.raises(ValueError, match="Bad Ethereum address"):
        PriceLevelsMessage(
            chainId=1,
            baseToken=cast(ChecksumAddress, "invalid"),
            quoteToken=to_checksum_address("0x0000000000000000000000000000000000000000"),
            levels=[],
        )


@pytest.mark.asyncio
async def test_publisher_publish_price_levels():
    """Test that LiquoricePriceLevelPublisher publishes price levels periodically."""
    out_quotes_q = asyncio.Queue()
    markets_mock = MagicMock()

    publisher = LiquoricePriceLevelPublisher(out_quotes_q, markets_mock)

    # Mock tokens
    # Token 1: USDC (Quote that we hold)
    usdc_token = MagicMock()
    usdc_token.symbol = "USDC"
    usdc_token.address = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
    usdc_token.balance = 5.0  # 5 USDC

    # Token 2: USDT (Base candidate)
    usdt_token = MagicMock()
    usdt_token.symbol = "USDT"
    usdt_token.address = "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9"
    usdt_token.balance = 0

    # Mock MarketState.get_tokens_by_chain_id
    # Should return a list of tokens
    markets_mock.get_tokens_by_chain_id.return_value = iter([usdc_token, usdt_token])

    # Mock shortest_path to return a valid path between USDT (base) and USDC (quote)
    markets_mock.shortest_path.return_value = [usdt_token, usdc_token]
    markets_mock.graph.get_edge_data.return_value = {"weight": 1.0}

    # Patch asyncio.sleep to break the loop or run once
    # We allow one iteration then raise CancelledError to stop the loop cleanly
    with patch(
        "app.protocols.liquorice.price_levels.asyncio.sleep", side_effect=asyncio.CancelledError
    ):
        try:
            await publisher.publish_price_levels()
        except asyncio.CancelledError:
            # Expected: we cancel the publish loop in tests to exit after one iteration.
            pass

    # Verify outputs
    assert not out_quotes_q.empty()

    # We expect one message: Base=USDT, Quote=USDC (since we have USDC balance)
    msg = await out_quotes_q.get()
    assert isinstance(msg, PriceLevelsMessage)

    # Check content

    assert msg.baseToken == "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9"
    assert msg.quoteToken == "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
    assert msg.levels[0].price == "1.00"
    assert msg.levels[0].amount == "5.0"
