"""A service to handle RFQs and send quotes"""

import asyncio
from dataclasses import dataclass
from decimal import Decimal
from logging import getLogger
from typing import AsyncIterator, Callable

from hexbytes import HexBytes
from web3 import Web3

from app.evm.const import ERC20_ZERO_ADDRESS
from app.markets.markets import MarketState
from app.metrics.metrics import metrics

# Premium multiplier for stablecoin's default rate to force the quoter
# to always quote slightly above 1:1 for testing purposes.
# In real-world usage this should be adjusted based on market conditions.
from app.protocols.liquorice.const import QUOTE_PREMIUM
from app.protocols.liquorice.schemas import (
    PriceLevelsMessage,
    QuoteLevelLite,
    RFQMessage,
    RFQQuoteMessage,
)
from app.protocols.liquorice.signer import Web3Signer
from app.schemas.token import ERC20Token

ZERO_ADDRESS = Web3.to_checksum_address(ERC20_ZERO_ADDRESS)


def scaled_base_token_raw_amount(
    base_token_amount_decimal: Decimal,
    market_quote_token_amount: Decimal,
    send_quote_token_amount: Decimal,
    decimal_to_raw: Callable[[Decimal], int],
) -> int:
    """Scale base token amount proportionally to reduced quote amount."""
    return decimal_to_raw(
        base_token_amount_decimal * (send_quote_token_amount / market_quote_token_amount)
    )


log = getLogger(__name__)


@dataclass(frozen=True)
class _RfqContext:
    rfq: RFQMessage
    base_token: ERC20Token
    quote_token: ERC20Token
    metrics_labels: dict
    price: Decimal


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

    def _get_tokens(self, rfq: RFQMessage, metrics_labels: dict):
        base_token = self.markets.get_token(rfq.baseToken, rfq.chainId)
        if not base_token:
            log.info(
                "BaseToken %s unsupported. Ignoring RFQ: %s",
                rfq.baseToken,
                rfq.rfqId,
            )
            metrics.rfqs_total.labels(**metrics_labels, status="UNSUPPORTED_BT").inc()
            return None

        quote_token = self.markets.get_token(rfq.quoteToken, rfq.chainId)
        if not quote_token:
            log.info(
                "QuoteToken %s unsupported. Ignoring RFQ: %s",
                rfq.quoteToken,
                rfq.rfqId,
            )
            metrics.rfqs_total.labels(**metrics_labels, status="UNSUPPORTED_QT").inc()
            return None

        return base_token, quote_token

    def _rfq_amounts(self, ctx: _RfqContext) -> tuple[int, int] | None:
        if ctx.price <= 0:
            log.info("No path found for RFQ %s", ctx.rfq.rfqId)
            metrics.rfqs_total.labels(**ctx.metrics_labels, status="NO_PATH").inc()
            return None

        base_raw_amount: int | None = None
        quote_raw_amount: int | None = None
        base_decimal: Decimal | None = None
        quote_decimal: Decimal | None = None

        if ctx.rfq.baseTokenAmount is not None:
            base_raw_amount = int(ctx.rfq.baseTokenAmount)
            if base_raw_amount <= 0:
                metrics.rfqs_total.labels(**ctx.metrics_labels, status="BAD_AMOUNT").inc()
                return None

            base_decimal = ctx.base_token.raw_to_decimal(base_raw_amount)
            quote_decimal = base_decimal * ctx.price
        else:
            quote_raw_amount = int(ctx.rfq.quoteTokenAmount or 0)
            if quote_raw_amount <= 0:
                metrics.rfqs_total.labels(**ctx.metrics_labels, status="BAD_AMOUNT").inc()
                return None

            quote_decimal = ctx.quote_token.raw_to_decimal(quote_raw_amount)

        if quote_decimal is None:
            log.info("No quote tokens available for RFQ %s: %s", ctx.rfq.rfqId, ctx.rfq.quoteToken)
            metrics.rfqs_total.labels(**ctx.metrics_labels, status="LOW_QT_BALANCE").inc()
            return None

        balance_decimal = ctx.quote_token.balance
        if balance_decimal <= 0:
            log.info(
                "No quote tokens available for RFQ %s: %s",
                ctx.rfq.rfqId,
                ctx.rfq.quoteToken,
            )
            metrics.rfqs_total.labels(**ctx.metrics_labels, status="LOW_QT_BALANCE").inc()
            return None

        if quote_decimal > balance_decimal:
            send_quote_decimal = balance_decimal
            if base_decimal is not None:
                base_raw_amount = scaled_base_token_raw_amount(
                    base_decimal,
                    quote_decimal,
                    send_quote_decimal,
                    ctx.base_token.decimal_to_raw,
                )
            quote_decimal = send_quote_decimal
            quote_raw_amount = ctx.quote_token.decimal_to_raw(quote_decimal)

        if base_raw_amount is None:
            base_decimal = quote_decimal / ctx.price
            base_raw_amount = ctx.base_token.decimal_to_raw(base_decimal)

        if quote_raw_amount is None:
            quote_raw_amount = ctx.quote_token.decimal_to_raw(quote_decimal)

        return (base_raw_amount, quote_raw_amount)

    def _build_signed_quote(
        self,
        ctx: _RfqContext,
        base_raw_amount: int,
        quote_raw_amount: int,
    ) -> RFQQuoteMessage | None:

        quote_lvl = QuoteLevelLite(
            baseToken=ctx.base_token.address,
            quoteToken=ctx.quote_token.address,
            baseTokenAmount=int(base_raw_amount),
            quoteTokenAmount=int(quote_raw_amount),
            expiry=ctx.rfq.expiry + 30,
            settlementContract=ZERO_ADDRESS,
            minQuoteTokenAmount=1,
            signer=ZERO_ADDRESS,
            recipient=ZERO_ADDRESS,
            signature=HexBytes("00" * 65),
        )
        non_signed_quote = RFQQuoteMessage(rfqId=ctx.rfq.rfqId, levels=[quote_lvl])
        return self.signer.sign_quote_levels(ctx.rfq, non_signed_quote)

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
                tokens = self._get_tokens(rfq, metrics_labels)
                if not tokens:
                    continue
                base_token, quote_token = tokens

                price = self._pair_price(base_token, quote_token)
                ctx = _RfqContext(
                    rfq=rfq,
                    base_token=base_token,
                    quote_token=quote_token,
                    metrics_labels=metrics_labels,
                    price=price,
                )
                amounts = self._rfq_amounts(ctx)
                if not amounts:
                    continue
                base_raw_amount, quote_raw_amount = amounts

                signed_quote = self._build_signed_quote(ctx, base_raw_amount, quote_raw_amount)
                if not signed_quote:
                    log.error("Failed to sign quote for RFQ: %s", rfq.rfqId)
                    metrics.rfqs_total.labels(**metrics_labels, status="SIGN_FAIL").inc()
                    continue
                log.info("Sending quote for RFQ %s: %s", rfq.rfqId, signed_quote)
                await self.out_quotes.put(signed_quote)
                metrics.rfqs_total.labels(**metrics_labels, status="QUOTE_SENT").inc()
            except Exception:  # pylint: disable=broad-exception-caught
                log.exception("Failed to process RFQ: %s", rfq.rfqId)
                metrics.rfqs_total.labels(**metrics_labels, status="ERROR").inc()
            finally:
                self.in_rfqs.task_done()
