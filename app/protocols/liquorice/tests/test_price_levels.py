
import asyncio
from unittest.mock import MagicMock, patch

import pytest
from app.protocols.liquorice.schemas import (
    PriceLevelLite,
    PriceLevelsMessage,
)
from app.quoter.quoter import LiquoriceQuoter


@pytest.mark.asyncio
async def test_price_levels_schema():
    """Verify PriceLevelsMessage serialization and validation."""
    level = PriceLevelLite(price="1", amount="1000.5")
    msg = PriceLevelsMessage(
        chainId=42161,
        baseToken="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",  # USDC
        quoteToken="0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",  # USDT
        levels=[level]
    )
    
    assert msg.chainId == 42161
    assert len(msg.levels) == 1
    assert msg.levels[0].price == "1"
    assert msg.levels[0].amount == "1000.5"
    
    # Test address validation
    with pytest.raises(ValueError, match="Bad Ethereum address"):
        PriceLevelsMessage(
            chainId=1,
            baseToken="invalid",
            quoteToken="0x0000000000000000000000000000000000000000",
            levels=[]
        )


@pytest.mark.asyncio
async def test_quoter_publish_price_levels():
    """Test that LiquoriceQuoter publishes price levels periodically."""
    in_rfqs_q = asyncio.Queue()
    out_quotes_q = asyncio.Queue()
    markets_mock = MagicMock()
    signer_mock = MagicMock()
    
    quoter = LiquoriceQuoter(in_rfqs_q, out_quotes_q, markets_mock, signer_mock)
    
    # Mock tokens
    # Token 1: USDC (Quote that we hold)
    usdc_token = MagicMock()
    usdc_token.symbol = "USDC"
    usdc_token.address = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
    usdc_token.balance = 5000000 # 5 USDC
    usdc_token.raw_to_decimal.return_value = 5.0
    
    # Token 2: USDT (Base candidate)
    usdt_token = MagicMock()
    usdt_token.symbol = "USDT"
    usdt_token.address = "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9"
    usdt_token.balance = 0 
    
    # Mock MarketState.get_tokens_by_chain_id
    # Should return a list of tokens
    markets_mock.get_tokens_by_chain_id.return_value = iter([usdc_token, usdt_token])
    
    # Patch asyncio.sleep to break the loop or run once
    # We allow one iteration then raise CancelledError to stop the loop cleanly
    with patch("asyncio.sleep", side_effect=asyncio.CancelledError) as mock_sleep:
        try:
            await quoter.publish_price_levels()
        except asyncio.CancelledError:
            pass
    
    # Verify outputs
    assert not out_quotes_q.empty()
    
    # We expect one message: Base=USDT, Quote=USDC (since we have USDC balance)
    msg = await out_quotes_q.get()
    assert isinstance(msg, PriceLevelsMessage)
    
    # Check content

    
    assert msg.baseToken == "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9"
    assert msg.quoteToken == "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
    assert msg.levels[0].price == "1"
    assert msg.levels[0].amount == "5.0"
