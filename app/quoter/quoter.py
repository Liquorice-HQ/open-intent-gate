"""A service to handle RFQs and send quotes"""

import asyncio
from contextlib import suppress
from decimal import Decimal, InvalidOperation
from logging import getLogger

from app.markets.markets import MarketState
from app.protocols.liquorice.schemas import (
    PriceLevelLite,
    PriceLevelsMessage,
    RFQMessage,
)
from app.protocols.liquorice.signer import Web3Signer

SUPPORTED_CHAIN_IDS = (42161, 1)

# Premium multiplier for stablecoin's default rate to force the quoter
# to always quote slightly above 1:1 for testing purposes.
# In real-world usage this should be adjusted based on market conditions.
QUOTE_PREMIUM = Decimal("1.0")

log = getLogger(__name__)


class LiquoriceQuoter:
    """Responder service singleton that reads RFQs from a queue
    and sends quotes back (if quoting conditions satisfy)"""

    in_rfqs: asyncio.Queue[RFQMessage]
    out_quotes: asyncio.Queue[PriceLevelsMessage]
    markets: MarketState
    signer: Web3Signer

    def __init__(
        self,
        in_rfqs: asyncio.Queue[RFQMessage],
        out_quotes: asyncio.Queue[PriceLevelsMessage],
        markets: MarketState,
        signer: Web3Signer,
    ) -> None:
        self.in_rfqs = in_rfqs
        self.in_rfqs = in_rfqs
        self.out_quotes = out_quotes
        self.markets = markets
        self.signer = signer

    async def publish_price_levels(self) -> None:
        """Periodically publish price levels for supported token pairs."""
        log.info("Starting to publish price levels")
        while True:
            for chain_id in SUPPORTED_CHAIN_IDS:
                tokens = list(self.markets.get_tokens_by_chain_id(chain_id))
                # Group by symbol or address to identify potential pairs
                # Logic: For each token Q (quote) we hold balance > 0:
                #   Find all other compatible tokens B (base) on the same chain.
                #   Publish PriceLevels for pair B/Q (Base=B, Quote=Q).

                # Optimization: Filter for stablecoins only as per requirements
                stablecoins = [
                    t
                    for t in tokens
                    if t.symbol in ["USDC", "USDT"]
                    # In a real app we might check contract address, but symbol is proxy here
                ]

                for quote_token in stablecoins:
                    try:
                        balance_raw = quote_token.balance
                    except AttributeError as err:
                        log.warning(
                            "Token %s missing decimals or balance on chain %s: %s",
                            quote_token.symbol,
                            chain_id,
                            err,
                        )
                        continue

                    if balance_raw <= 0:
                        continue

                    try:
                        amount_str = str(quote_token.raw_to_decimal(int(balance_raw)))
                    except (InvalidOperation, TypeError, ValueError) as err:
                        log.error(
                            "Unable to convert balance for %s on chain %s: %s",
                            quote_token.symbol,
                            chain_id,
                            err,
                        )
                        continue

                    for base_token in stablecoins:
                        if base_token.address == quote_token.address:
                            continue

                        # Create and enqueue PriceLevelsMessage
                        # Pair: Base (trader sells) -> Quote (trader buys / we sell)
                        # We sell Quote token.

                        try:
                            level = PriceLevelLite(price="1", amount=amount_str)
                            msg = PriceLevelsMessage(
                                chainId=chain_id,
                                baseToken=base_token.address,
                                quoteToken=quote_token.address,
                                levels=[level],
                            )
                        except ValueError as err:
                            log.error(
                                "Invalid price level for %s/%s on chain %s: %s",
                                base_token.symbol,
                                quote_token.symbol,
                                chain_id,
                                err,
                            )
                            continue

                        await self.out_quotes.put(msg)
                        log.debug(
                            "Published level for %s/%s on chain %s: %s",
                            base_token.symbol,
                            quote_token.symbol,
                            chain_id,
                            level,
                        )

            await asyncio.sleep(1)

    async def run(self) -> None:
        """Start the publisher loop."""
        with suppress(asyncio.CancelledError):
            await self.publish_price_levels()
