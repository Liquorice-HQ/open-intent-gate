import asyncio
from pathlib import Path
from typing import AsyncIterator, List
from unittest.mock import AsyncMock, patch
from uuid import UUID

import pytest
from eth_typing import HexStr
from web3.main import to_checksum_address

from app.config.maker import MakerConfig
from app.protocols.liquorice.client import LiquoriceClient
from app.protocols.liquorice.schemas import (
    LiquoriceEnvelope,
    MessageType,
    PriceLevelLite,
    PriceLevelsMessage,
    RFQMessage,
)


class MockWsConnection:
    """This class simulates a WebSocket connection by:
    - Receiving predefined messages from a queue
    - Sending messages back through the connection
    - Tracking message completion with events

    Attributes:
        sent (List[str]): Messages that have been actually sent through the connection
        msgs_expected_to_be_sent (List[str]): Messages expected to be sent (now only counted)
        msg_all_received (asyncio.Event): Set when all messages are received
        msg_all_sent (asyncio.Event): Set when all expected messages are sent
        _recv (asyncio.Queue[str]): Queue containing messages to be received
    """

    def __init__(
        self, msgs_to_receive: List[str], msgs_expected_to_be_sent: List[str] = []
    ) -> None:
        self.sent = []
        self.msgs_expected_to_be_sent = msgs_expected_to_be_sent
        self.msg_all_received = asyncio.Event()
        self.msg_all_sent = asyncio.Event()
        self._recv = asyncio.Queue()  # Queue for incoming RFQs (to receive from mocked WebSocket)
        for msg in msgs_to_receive:
            self._recv.put_nowait(msg)

    def __aiter__(self) -> AsyncIterator[str]:
        async def iter_msgs() -> AsyncIterator[str]:
            while not self._recv.empty():
                yield await self._recv.get()
            self.msg_all_received.set()

        return iter_msgs()

    async def send(self, msg) -> None:
        self.sent.append(msg)
        if len(self.sent) >= len(self.msgs_expected_to_be_sent):
            self.msg_all_sent.set()


connected_text = (Path(__file__).parent / "data" / "connected_msg.json").read_text()
rfq_text = (Path(__file__).parent / "data" / "liquorice_rfq.json").read_text()
price_levels_msg = PriceLevelsMessage(
    chainId=42161,
    baseToken=to_checksum_address("0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9"),
    quoteToken=to_checksum_address("0xaf88d065e77c8cC2239327C5EDb3A432268e5831"),
    levels=[
        PriceLevelLite(price="1.2345", amount="1000000000000000000"),
        PriceLevelLite(price="1.2350", amount="500000000000000000"),
    ],
)
expected_price_levels_raw_msg = LiquoriceEnvelope(
    message=price_levels_msg, messageType=MessageType.PRICE_LEVELS
).model_dump_json(exclude_none=True)


@pytest.mark.asyncio
async def test_liquorice_client_run_relays_messages():
    """Test the LiquoriceClient's run method relays messages between respective queues and websocket."""
    client = LiquoriceClient(
        MakerConfig(maker="maker_name", authorization="auth", signer_priv_key=HexStr("0x00"))
    )

    # Mock WebSocket connection
    ws_mock = MockWsConnection(
        msgs_to_receive=[connected_text, rfq_text],
        msgs_expected_to_be_sent=[expected_price_levels_raw_msg],
    )

    # Put a realistic price levels response into the outbound queue
    await client.in_quotes.put(price_levels_msg)

    with patch(
        "app.protocols.liquorice.client.websockets.connect",
        return_value=AsyncMock(__aenter__=AsyncMock(return_value=ws_mock)),
    ):

        task = asyncio.create_task(client.run())

        await asyncio.wait_for(ws_mock.msg_all_received.wait(), timeout=1)
        await asyncio.wait_for(ws_mock.msg_all_sent.wait(), timeout=1)

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert not client.out_rfqs.empty()
        rfq = await client.out_rfqs.get()
        assert isinstance(rfq, RFQMessage)
        assert rfq.solverRfqId == UUID("95a0f428-a6c4-4207-81b2-e47436741e9b")
        assert rfq.rfqId == UUID("846063db-1769-438b-8002-00fd981603df")
        assert rfq.chainId == 42161
        assert rfq.solver == "portus"

        assert client.in_quotes.empty()
        assert len(ws_mock.sent) == 1
        assert ws_mock.sent[0] == expected_price_levels_raw_msg
