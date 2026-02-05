import asyncio
from logging import getLogger

import websockets
from pydantic import ValidationError
from websockets.asyncio.client import ClientConnection

from app.config.maker import MakerConfig

from .schemas import (
    LiquoriceEnvelope,
    MessageType,
    PriceLevelsMessage,
    RFQMessage,
    RFQQuoteMessage,
)

LIQUORICE_WS_URL = "wss://api.liquorice.tech/v1/maker/ws"

log = getLogger(__name__)


class LiquoriceClient:
    """Client for connecting to the Liquorice WebSocket API.
    Relays RFQs and quotes between the queues and the WebSocket."""

    out_rfqs: asyncio.Queue[RFQMessage]
    in_quotes: asyncio.Queue[PriceLevelsMessage | RFQQuoteMessage]

    def __init__(self, cfg_maker: MakerConfig) -> None:
        self.uri = LIQUORICE_WS_URL
        self.headers = {
            "maker": cfg_maker.maker,
            "authorization": cfg_maker.authorization,
        }
        self.out_rfqs: asyncio.Queue[RFQMessage] = asyncio.Queue()  # Queue for outgoing RFQs
        self.in_quotes: asyncio.Queue[PriceLevelsMessage | RFQQuoteMessage] = (
            asyncio.Queue()
        )  # Queue for incoming quotes / price levels

    async def _reader(self, ws: ClientConnection) -> None:
        """Reads messages from the WebSocket and puts them into the rfqs queue."""
        async for message in ws:
            try:
                log.debug("Rcvd: %s", message)
                rfq = LiquoriceEnvelope.model_validate_json(message)
                if rfq.messageType == MessageType.CONNECTED:
                    log.debug("Message type CONNECTED received, ignoring")
                    continue
                elif rfq.messageType == MessageType.RFQ:
                    log.debug("Message type RFQ received, processing")
                    await self.out_rfqs.put(rfq.message)
                else:
                    log.warning("Unexpected message type Rcvd: %s", rfq.messageType)
            except ValidationError as e:
                log.error("Validation error: %s", e)
                continue

    async def _writer(self, ws: ClientConnection) -> None:
        """Reads quote from the quotes queue and sends them over the WebSocket."""
        while True:
            msg = await self.in_quotes.get()
            if isinstance(msg, RFQQuoteMessage):
                raw_msg = LiquoriceEnvelope(
                    message=msg, messageType=MessageType.RFQ_QUOTE
                ).model_dump_json(exclude_none=True)
            elif isinstance(msg, PriceLevelsMessage):
                raw_msg = LiquoriceEnvelope(
                    message=msg, messageType=MessageType.PRICE_LEVELS
                ).model_dump_json(exclude_none=True)
            else:
                log.error("Unexpected message type in out_quotes: %s", type(msg))
                continue
            await ws.send(raw_msg)
            log.debug("Sent: %s", raw_msg)

    async def run(self) -> None:
        """Connects to the Liquorice WebSocket and starts reading and writing messages."""
        async with websockets.connect(self.uri, additional_headers=self.headers) as ws:
            log.info("Connected to Liquorice WebSocket at %s", self.uri)
            # Start the reader and writer tasks
            await asyncio.gather(self._reader(ws), self._writer(ws))
