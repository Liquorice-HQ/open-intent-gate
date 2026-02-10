"""A service to publish price levels for Liquorice protocol"""

import asyncio
import logging
import os
from contextlib import suppress
from decimal import Decimal, InvalidOperation

from app.markets.markets import MarketState
from app.protocols.liquorice.const import QUOTE_PREMIUM
from app.protocols.liquorice.schemas import (
    PriceLevelLite,
    PriceLevelsMessage,
    RFQQuoteMessage,
)

log = logging.getLogger(__name__)

SUPPORTED_CHAIN_IDS = (42161, 1)


class LiquoricePriceLevelPublisher:
    """Service that periodically publishes price levels for supported token pairs."""

    out_quotes: asyncio.Queue[PriceLevelsMessage | RFQQuoteMessage]
    markets: MarketState
    price_level_publish_interval: float

    def __init__(
        self,
        out_quotes: asyncio.Queue[PriceLevelsMessage | RFQQuoteMessage],
        markets: MarketState,
    ) -> None:
        self.out_quotes = out_quotes
        self.markets = markets
        self.price_level_publish_interval = self._get_price_level_publish_interval()

    @staticmethod
    def _get_price_level_publish_interval() -> float:
        try:
            interval = float(os.getenv("PRICE_LEVEL_PUBLISH_INTERVAL", "1.0"))
        except ValueError:
            log.warning(
                "Invalid PRICE_LEVEL_PUBLISH_INTERVAL value: %s. Using default of 1.0 seconds.",
                os.getenv("PRICE_LEVEL_PUBLISH_INTERVAL"),
            )
            interval = 1.0

        log.info("Price level publish interval: %s seconds", interval)
        return interval

    def _pair_price(self, base_token, quote_token) -> Decimal:
        """Compute price for base/quote using market weights and QUOTE_PREMIUM."""
        path = self.markets.shortest_path(base_token, quote_token)
        if not path:
            return Decimal("0")

        price = Decimal("1")
        for i in range(len(path) - 1):
            edge = self.markets.graph.get_edge_data(path[i], path[i + 1])
            weight = edge.get("weight", 1.0) if edge else 1.0
            price *= Decimal(str(weight))

        return price * QUOTE_PREMIUM

    async def publish_price_levels(self) -> None:
        """Periodically publish price levels for supported token pairs."""
        log.info("Starting to publish price levels")
        while True:
            try:
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
                            balance_decimal = quote_token.balance
                        except AttributeError as err:
                            log.warning(
                                "Token %s missing decimals or balance on chain %s: %s",
                                quote_token.symbol,
                                chain_id,
                                err,
                            )
                            continue

                        if balance_decimal <= 0:
                            continue

                        try:
                            amount_str = str(balance_decimal)
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

                            price = self._pair_price(base_token, quote_token)
                            if price <= 0:
                                continue

                            try:
                                level = PriceLevelLite(price=str(price), amount=amount_str)
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

            except Exception:  # pylint: disable=broad-exception-caught
                log.exception("Unexpected error in publish_price_levels")

            await asyncio.sleep(self.price_level_publish_interval)

    async def run(self) -> None:
        """Start price level publishing."""
        with suppress(asyncio.CancelledError):
            await self.publish_price_levels()
