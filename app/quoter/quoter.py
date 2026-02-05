"""A service to handle RFQs and send quotes"""

import asyncio
from typing import AsyncIterator
from contextlib import suppress
from decimal import Decimal, InvalidOperation
from logging import getLogger

from hexbytes import HexBytes
from web3 import Web3

from app.evm.const import ERC20_ZERO_ADDRESS
from app.markets.markets import MarketState
from app.metrics.metrics import metrics
from app.protocols.liquorice.schemas import (
    PriceLevelLite,
    PriceLevelsMessage,
    QuoteLevelLite,
    RFQMessage,
    RFQQuoteMessage,
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
    out_quotes: asyncio.Queue[PriceLevelsMessage | RFQQuoteMessage]
    markets: MarketState
    signer: Web3Signer

    def __init__(
        self,
        in_rfqs: asyncio.Queue[RFQMessage],
        out_quotes: asyncio.Queue[PriceLevelsMessage | RFQQuoteMessage],
        markets: MarketState,
        signer: Web3Signer,
    ) -> None:
        self.in_rfqs = in_rfqs
        self.out_quotes = out_quotes
        self.markets = markets
        self.signer = signer

    async def rfq_stream(self) -> AsyncIterator[RFQMessage]:
        """Yield RFQs from the inbound queue."""
        while True:
            rfq = await self.in_rfqs.get()
            yield rfq

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

    async def process_rfqs(self) -> None:
        """Process RFQs from queue and send signed quotes."""
        log.info("Starting to process RFQs")
        async for rfq in self.rfq_stream():
            metrics_labels = {
                "chain_id": rfq.chainId,
                "solver": rfq.solver or "unknown",
                "base_token": rfq.baseToken,
                "quote_token": rfq.quoteToken,
            }
            try:
                log.debug("Processing RFQ: %s", rfq)
                base_token = self.markets.get_token(rfq.baseToken, rfq.chainId)
                if not base_token:
                    log.info(
                        "BaseToken %s unsupported. Ignoring RFQ: %s",
                        rfq.baseToken,
                        rfq.rfqId,
                    )
                    metrics.rfqs_total.labels(**metrics_labels, status="UNSUPPORTED_BT").inc()
                    continue

                quote_token = self.markets.get_token(rfq.quoteToken, rfq.chainId)
                if not quote_token:
                    log.info(
                        "QuoteToken %s unsupported. Ignoring RFQ: %s",
                        rfq.quoteToken,
                        rfq.rfqId,
                    )
                    metrics.rfqs_total.labels(**metrics_labels, status="UNSUPPORTED_QT").inc()
                    continue

                price = self._pair_price(base_token, quote_token)
                if price <= 0:
                    log.info("No path found for RFQ %s", rfq.rfqId)
                    metrics.rfqs_total.labels(**metrics_labels, status="NO_PATH").inc()
                    continue

                if rfq.baseTokenAmount is not None:
                    base_raw_amount = int(rfq.baseTokenAmount)
                    if base_raw_amount <= 0:
                        metrics.rfqs_total.labels(**metrics_labels, status="BAD_AMOUNT").inc()
                        continue
                    base_decimal = base_token.raw_to_decimal(base_raw_amount)
                    quote_decimal = base_decimal * price
                    if quote_decimal > quote_token.balance:
                        log.info(
                            "No quote tokens available for RFQ %s: %s",
                            rfq.rfqId,
                            rfq.quoteToken,
                        )
                        metrics.rfqs_total.labels(**metrics_labels, status="LOW_QT_BALANCE").inc()
                        continue
                    quote_raw_amount = quote_token.decimal_to_raw(quote_decimal)
                else:
                    quote_raw_amount = int(rfq.quoteTokenAmount or 0)
                    if quote_raw_amount <= 0:
                        metrics.rfqs_total.labels(**metrics_labels, status="BAD_AMOUNT").inc()
                        continue
                    quote_decimal = quote_token.raw_to_decimal(quote_raw_amount)
                    if quote_decimal > quote_token.balance:
                        log.info(
                            "No quote tokens available for RFQ %s: %s",
                            rfq.rfqId,
                            rfq.quoteToken,
                        )
                        metrics.rfqs_total.labels(**metrics_labels, status="LOW_QT_BALANCE").inc()
                        continue
                    base_decimal = quote_decimal / price
                    base_raw_amount = base_token.decimal_to_raw(base_decimal)

                zero_address = Web3.to_checksum_address(ERC20_ZERO_ADDRESS)
                quote_lvl = QuoteLevelLite(
                    baseToken=base_token.address,
                    quoteToken=quote_token.address,
                    baseTokenAmount=int(base_raw_amount),
                    quoteTokenAmount=int(quote_raw_amount),
                    expiry=rfq.expiry + 30,
                    settlementContract=zero_address,
                    minQuoteTokenAmount=1,
                    signer=zero_address,
                    recipient=zero_address,
                    signature=HexBytes("00" * 65),
                )
                non_signed_quote = RFQQuoteMessage(rfqId=rfq.rfqId, levels=[quote_lvl])
                signed_quote = self.signer.sign_quote_levels(rfq, non_signed_quote)
                if not signed_quote:
                    log.error("Failed to sign quote for RFQ: %s", rfq.rfqId)
                    metrics.rfqs_total.labels(**metrics_labels, status="SIGN_FAIL").inc()
                    continue
                log.info("Sending quote for RFQ %s: %s", rfq.rfqId, signed_quote)
                await self.out_quotes.put(signed_quote)
                metrics.rfqs_total.labels(**metrics_labels, status="QUOTE_SENT").inc()

            except Exception as e:  # pylint: disable=broad-exception-caught
                log.error("Failed to process RFQ: %s", e)
                metrics.rfqs_total.labels(**metrics_labels, status="QUOTER_UNHANDLED_EXC").inc()

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

            await asyncio.sleep(1)

    async def run(self) -> None:
        """Start RFQ processing and price level publishing."""
        with suppress(asyncio.CancelledError):
            await asyncio.gather(self.process_rfqs(), self.publish_price_levels())
