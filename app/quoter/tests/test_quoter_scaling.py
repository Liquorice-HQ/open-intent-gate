import asyncio
from decimal import Decimal
from unittest.mock import Mock
from uuid import uuid4

import pytest
from hexbytes import HexBytes
from web3.main import to_checksum_address

from app.markets.markets import MarketState
from app.protocols.liquorice.schemas import RFQMessage
from app.quoter.quoter import (
    QUOTE_PREMIUM,
    LiquoriceQuoter,
    scaled_base_token_raw_amount,
)
from app.schemas.token import ERC20Token


@pytest.fixture
def mock_market_state():
    ms = Mock(spec=MarketState)
    ms.graph = Mock()
    return ms


@pytest.fixture
def quoter(mock_market_state):
    in_q = asyncio.Queue()
    out_q = asyncio.Queue()
    signer = Mock()
    signer.sign_quote_levels = Mock(
        side_effect=lambda rfq, quote: quote
    )  # Return unsigned quote for inspection
    return LiquoriceQuoter(in_q, out_q, mock_market_state, signer)


@pytest.mark.asyncio
async def test_quote_scaling_insufficient_liquidity(quoter, mock_market_state):
    # Setup tokens
    base_token = Mock(spec=ERC20Token)
    base_token.address = "0xBase"
    base_token.raw_to_decimal.side_effect = Decimal
    base_token.decimal_to_raw.side_effect = int

    quote_token = Mock(spec=ERC20Token)
    quote_token.address = "0xQuote"
    # Balance is only 50
    quote_token.balance = Decimal("50")
    quote_token.decimal_to_raw.side_effect = int

    rfq_base_amount = 1000

    # Use valid checksummed addresses
    # We use to_checksum_address to ensure they are valid for the strict schema validation
    base_token_addr = to_checksum_address("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")  # WETH
    quote_token_addr = to_checksum_address("0xA0b86a33E6441E72bF7f0a29c0DFD33F5b4f7F45")  # USDC
    trader_addr = to_checksum_address("0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045")  # Vitalik

    # Update mocks with these addresses
    base_token.address = base_token_addr
    quote_token.address = quote_token_addr
    mock_market_state.get_token.side_effect = lambda addr, chain: (
        base_token if addr == base_token_addr else quote_token
    )
    mock_market_state.shortest_path.return_value = [base_token, quote_token]
    mock_market_state.graph.get_edge_data.return_value = {"weight": 1.0}

    rfq = RFQMessage(
        chainId=1,
        rfqId=uuid4(),
        baseToken=base_token_addr,
        quoteToken=quote_token_addr,
        baseTokenAmount=rfq_base_amount,
        trader=trader_addr,
        effectiveTrader=trader_addr,
        expiry=1800000000,
        nonce=HexBytes("0x" + "00" * 32),
        solver="solver",
        solverRfqId=uuid4(),
    )

    await quoter.in_rfqs.put(rfq)

    # Execute one pass of RFQ processing logic only.
    task = asyncio.create_task(quoter.process_rfqs())

    try:
        # Wait for quote
        quote = await asyncio.wait_for(quoter.out_quotes.get(), timeout=2.0)
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            # Suppress the expected cancellation when stopping the background quoter task.
            pass

    level = quote.levels[0]

    # Assertions
    # With only 50 quote tokens available, the quoter should scale the base amount
    # so that the resulting quote respects available liquidity and the quote premium.
    assert level.quoteTokenAmount == 50

    # The correct behavior should satisfy approximately:
    # baseTokenAmount * QUOTE_PREMIUM ~= quoteTokenAmount
    # For this test setup with balance 50 and QUOTE_PREMIUM == 1.0:
    # 50 * 1.0 = 50.

    expected_base_amount = int(quote_token.balance / QUOTE_PREMIUM)
    assert level.baseTokenAmount == expected_base_amount


def test_scaled_base_amount_non_stable_rate():
    base_token_amount_decimal = Decimal("1")  # 1 WETH
    market_quote_token_amount = Decimal("3000")  # 3000 USDT
    send_quote_token_amount = Decimal("1500")  # 1500 USDT

    def decimal_to_raw(value: Decimal) -> int:
        return int(value * Decimal("1e18"))

    scaled_raw = scaled_base_token_raw_amount(
        base_token_amount_decimal,
        market_quote_token_amount,
        send_quote_token_amount,
        decimal_to_raw,
    )

    assert scaled_raw == int(Decimal("0.5") * Decimal("1e18"))
