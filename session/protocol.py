"""
protocol.py

Asyncio-based protocol that implements the CAL RPC session protocol.

todo: idle timeouts
todo: object callbacks
todo: object instantiation
"""


import random
import logging
import asyncio
from collections.abc import MutableMapping
from session.packet import PacketHeader, SequenceNumber, PacketFlags, U32, SessionPacketType, ChannelPacketType
from asyncio import transports
from typing import Final, Callable, Iterable, Iterator
from itertools import chain


class Channel:
    """
    A channel in the CAL session protocol.
    """
    def __init__(self, num: int, endpoint: 'Endpoint'):
        """
        Create a new channel.

        New channels are connected by default.

        :param num: number of the channel.
        :param endpoint: endpoint the channel belongs to.
        """
        self._num = num
        self._endpoint = endpoint
        self._connected = True

        self._logger = logging.getLogger(f"{self.__class__.__name__}{(self.number, self.endpoint)}")
        self._logger.setLevel(logging.DEBUG)

    @property
    def number(self) -> int:
        """
        Obtain the number of this channel.
        """
        return self._num

    @property
    def endpoint(self) -> 'Endpoint':
        """
        Obtain the endpoint the channel belongs to.
        """
        return self._endpoint

    def _close(self):
        """
        Close the channel.

        Used by the endpoint to close this channel.
        """
        self._connected = False
        # todo: add notifier to notify created objects of close

    def _ensure_connected(self):
        if not self.connected:
            raise RuntimeError("channel not connected")

    @property
    def connected(self) -> bool:
        return self._connected

    def _receive(self, data: bytes):
        """
        Receive application data for this channel.

        :param data: data received.
        """
        print("channel:", self.number, "rx:", data)

    def send(self, data: bytes):
        # noinspection PyProtectedMember
        self.endpoint._send_channel(self, data)

    async def close(self):
        self._ensure_connected()
        await self.endpoint.close_channel(self)


class ACKHandler(MutableMapping[tuple[U32, SequenceNumber], asyncio.Future]):
    """
    Class used for fulfilling futures that are waiting for the header and payload from an ACK.
    """

    def __init__(self):
        self._futures_by_channel: dict[int, dict[int, asyncio.Future]] = {}

    def __iter__(self) -> Iterator[asyncio.Future]:
        return chain.from_iterable(map(lambda v: v.values(), self._futures_by_channel))

    def __len__(self) -> int:
        return sum(map(len, self._futures_by_channel.values()))

    def __getitem__(self, key: tuple[U32, SequenceNumber]) -> asyncio.Future:
        """
        Lookup a future.

        :param key: channel and sequence number that the future corresponds to.
        """
        cv = key[0].value
        sv = key[1].value

        if cv not in self._futures_by_channel:
            raise KeyError(key)

        futures_by_sequence = self._futures_by_channel[cv]
        if sv not in futures_by_sequence:
            raise KeyError(key)

        return futures_by_sequence[sv]

    def __setitem__(self, key: tuple[U32, SequenceNumber], fut: asyncio.Future):
        """
        Add a future to the handler.

        :param key: channel and sequence number that the future corresponds to.
        :param fut: future to add.
        """
        cv = key[0].value
        sv = key[1].value

        futures_by_sequence = self._futures_by_channel.setdefault(cv, {})
        if sv in futures_by_sequence:
            raise KeyError(f"future already registered for sequence {sv} on channel {cv}")

        # todo: could be made better: i.e. use future id to ensure we delete the right one
        # and nobody registered another one.
        fut.add_done_callback(lambda _: self.__delitem__(key))
        futures_by_sequence[sv] = fut

    def __delitem__(self, key: tuple[U32, SequenceNumber]):
        """
        Remove a future from the handler.

        Note that this does not cancel the future!

        :param key: channel and sequence number that the future corresponds to.
        """
        cv = key[0].value
        sv = key[1].value

        if key not in self:
            raise KeyError(key)

        futures_by_sequence = self._futures_by_channel[cv]
        del futures_by_sequence[sv]
        if not len(futures_by_sequence):
            del self._futures_by_channel[cv]

        return True

    def invalidate(self, channels: Iterable[int] = None):
        """
        Invalidate futures waiting for ACKs on given channels, by cancelling them.

        :param channels: channels to invalidate futures for. Use ``None`` for all channels.
        """
        if channels is None:
            # iteration is fine because done callback isn't called immediately
            channels = iter(self._futures_by_channel)

        for channel in channels:
            if channel not in self._futures_by_channel:
                continue

            futures_by_seq = self._futures_by_channel[channel]
            for fut in futures_by_seq.values():
                # cancellation will automatically remove it
                # no issues with iteration because done callback isn't
                # called immediately.
                fut.cancel()

    def handle(self, hdr: PacketHeader, payload: bytes):
        """
        Handle an ACK packet.

        ACK packets that don't have futures corresponding to them are ignored.

        :param hdr: header of the packet.
        :param payload: packet payload.
        """
        key = hdr.channel, SequenceNumber.from_bytes(payload)
        if key not in self:
            return

        self[key].set_result((hdr, payload))


class Endpoint:
    """
    An endpoint in the CAL session protocol.
    """
    Address: Final = tuple[str, int]

    def __init__(self, addr: Address, session: 'Session'):
        """
        Initialize a new endpoint.

        :param addr: endpoint address.
        :param session: session protocol object this address is associated with.
        """
        self._loop = asyncio.get_running_loop()
        self._addr = addr
        self._session = session

        self._connected = False

        self._local_seq_rerandomize = True
        self._local_seq = SequenceNumber(0)
        self._remote_seq_check = False
        self._remote_seq = SequenceNumber(0)

        self._ack_handler = ACKHandler()
        self._channels: dict[int, Channel] = {}

        self._packet_handlers: dict[SessionPacketType, Callable[[PacketHeader, bytes], None]] = {
            SessionPacketType.OPEN_SESSION: self._handle_session_open,
            SessionPacketType.CLOSE_SESSION: self._handle_session_close,
            SessionPacketType.RESET_SESSION: self._handle_session_reset,
            ChannelPacketType.OPEN_CHANNEL: self._handle_channel_open,
            ChannelPacketType.CLOSE_CHANNEL: self._handle_channel_close,
            ChannelPacketType.RESET_CHANNEL: self._handle_channel_reset,
            ChannelPacketType.UL_PDU: self._handle_channel_ulpdu,
        }

        self._logger = logging.getLogger(f"{self.__class__.__name__}@{addr}")
        self._logger.setLevel(logging.DEBUG)

    def _send_raw(self, data: bytes):
        """
        Send raw data to the endpoint.

        :param data: data to send.
        """
        self._logger.debug(f"_send_raw: {data}")
        self._session._send(self._addr, data)

    def _send(self, hdr: PacketHeader, payload: bytes = b'') -> PacketHeader:
        """
        Send a message to the endpoint.

        The sequence number will be filled in automatically.

        :param hdr: message header.
        :param payload: message payload.
        :return: header actually used.
        """
        if self._local_seq_rerandomize:
            seq = self._local_seq.randomize()
            self._local_seq_rerandomize = False
        else:
            seq = self._local_seq.next()
        hdr.sequence = seq
        self._logger.debug(f"_send: {hdr}, {payload}")
        self._send_raw(hdr.to_bytes() + payload)
        return hdr

    def _send_channel(self, channel: Channel, payload: bytes):
        """
        Send application data over a channel.

        :param channel: channel to send over.
        :param payload: payload to send.
        """
        self._ensure_channel_connected(channel)
        self._send(PacketHeader(PacketFlags.NONE, U32(channel.number)), payload)

    async def _receive(self, data: bytes):
        """
        Handle packet received from an endpoint.

        :param data: packet data received.
        """
        self._logger.debug(f"_receive: {data}")
        try:
            hdr = PacketHeader.from_bytes(data)
        except ValueError:
            logging.warning(f"_receive: dropped packet with invalid header: {data}")
            return

        try:
            type_ = hdr.classify()
        except ValueError:
            logging.warning(f"_receive: dropped packet of unknown type: {hdr.flags}")
            return

        # reset session packets bypass the sequence check.
        if self._remote_seq_check and (type != SessionPacketType.RESET_SESSION):
            distance = self._remote_seq.distance(hdr.sequence)
            # todo: is this even right
            if distance > 2 ** 31:
                logging.warning(f"_receive: dropped packet with invalid sequence number: {hdr.sequence.value}")
                return

            self._remote_seq = hdr.sequence

        payload = data[hdr.LENGTH:]

        if hdr.ack:
            self._ack_handler.handle(hdr, payload)
            return

        logging.debug(f"_receive: received packet with type {type_}")
        self._packet_handlers[type_](hdr, payload)

    def _send_generic_ack(self, hdr: PacketHeader) -> PacketHeader:
        """
        Send a generic ACK to the remote endpoint based on a header from an ACKable packet.

        :param hdr: header from ACK-able.
        :return: header of ACK sent.
        """
        ack_hdr = PacketHeader(hdr.flags | PacketFlags.ACK, hdr.channel)
        return self._send(ack_hdr, hdr.sequence.to_bytes())

    def _handle_session_open(self, hdr: PacketHeader, _: bytes):
        self._logger.debug("_handle_session_open: opening session")
        # enable remote sequence check because remote side is active
        self._remote_seq_check = True
        self._remote_seq = hdr.sequence
        self._send_generic_ack(hdr)

        self._logger.debug("_handle_session_open: opened")
        self._connected = True

    def _handle_session_close(self, hdr: PacketHeader, _: bytes):
        self._logger.debug("_handle_session_close: closing session")
        self._send_generic_ack(hdr)
        self._handle_session_reset(hdr)
        self._logger.debug("_handle_session_close: closed")

    def _handle_session_reset(self, *_):
        self._logger.debug("_handle_session_reset: resetting session")
        self._connected = False
        self._local_seq_rerandomize = True
        self._remote_seq_check = False
        self._ack_handler.invalidate()
        self._logger.debug("_handle_session_reset: reset")

    def _handle_channel_open(self, hdr: PacketHeader, payload: bytes):
        service = payload.decode("UTF-8")
        self._logger.debug(f"_handle_channel_open: opening {service}")

        num = hdr.channel.value

        if num in self._channels:
            self.reset_channel(num)
            return
        try:
            self._channels[hdr.channel.value] = Channel(num, self)
        except Exception as e:
            self._logger.warning(f"_handle_channel_open: could not instantiate Channel: {e}")
            self.reset_channel(num)
            return

        self._send_generic_ack(hdr)
        self._logger.debug(f"_handle_channel_open: opened")

    def _handle_channel_close(self, hdr: PacketHeader, _: bytes):
        num = hdr.channel.value
        self._logger.debug(f"_handle_channel_close: closing {num}")

        if num not in self._channels:
            self.reset_channel(num)
            return

        self._send_generic_ack(hdr)
        self._handle_session_reset(hdr)
        self._logger.debug(f"_handle_channel_close: closed")

    def _handle_channel_reset(self, hdr: PacketHeader, *_):
        num = hdr.sequence.value
        chan = hdr.channel.value
        self._logger.debug(f"_handle_channel_reset: resetting {num}")

        if num in self._channels:
            # noinspection PyProtectedMember
            self._channels[num]._close()
            del self._channels[num]

        self._ack_handler.invalidate([chan])
        self._logger.debug(f"_handle_channel_reset: reset")

    def _handle_channel_ulpdu(self, hdr: PacketHeader, payload: bytes):
        num = hdr.channel.value
        if num not in self._channels:
            self.reset_channel(num)
            return

        # noinspection PyProtectedMember
        self._channels[num]._receive(payload)

    def _ensure_connected(self):
        """
        Ensure that the session is connected, raising an exception if it is not.
        """
        if not self.connected:
            raise RuntimeError("session not connected")

    def _ensure_channel_connected(self, channel: Channel):
        """
        Ensure that the channel is connected, raising an exception if it is not.

        :param channel: channel to ensure connectedness for.
        """
        # Session must be connected for channels to be connected.
        self._ensure_connected()
        if self._channels[channel.number] is not channel:
            raise RuntimeError("channel not connected")

    @property
    def connected(self) -> bool:
        return self._connected

    async def open_session(self):
        """
        Open a session.

        Cannot be done if the session is already connected.
        """
        if self.connected:
            raise RuntimeError("session connected")

        self._logger.debug("open_session: opening session")

        ack = self._loop.create_future()
        hdr = self._send(PacketHeader(PacketFlags.SYN))

        self._ack_handler[hdr.channel, hdr.sequence] = ack
        self._logger.debug("open_session: waiting for ACK")
        try:
            await ack
        except asyncio.CancelledError:
            self._handle_session_reset()

        ack_hdr, _ = ack.result()
        if not (ack_hdr.session and ack_hdr.classify() == SessionPacketType.OPEN_SESSION):
            self._handle_session_reset()
            raise RuntimeError("invalid response to open")

        self._logger.debug("open_session: opened")
        self._connected = True

    async def close_session(self):
        """
        Close a session.

        Must be connected.
        """
        self._ensure_connected()
        self._logger.debug("close_session: closing session")

        ack = self._loop.create_future()
        hdr = self._send(PacketHeader(PacketFlags.FIN))

        self._ack_handler[hdr.channel, hdr.sequence] = ack
        self._logger.debug("close_session: waiting for ACK")

        await ack

        ack_hdr, _ = ack.result()
        if ack_hdr.classify() is not SessionPacketType.CLOSE_SESSION:
            raise RuntimeError("invalid response to close")

        self._logger.debug("close_session: closed")
        self._handle_session_reset()

    def reset_session(self):
        """
        Reset a session (ungraceful close).
        """
        self._logger.debug("reset_session: resetting session")

        self._send(PacketHeader(PacketFlags.RST))

        self._logger.debug("reset_session: reset")
        self._handle_session_reset()

    async def open_channel(self, service: str) -> Channel:
        """
        Open a channel for a particular service.

        Must be connected.

        :param service: service to connect to.
        """
        self._ensure_connected()

        self._logger.debug(f"open_channel: opening {service}")
        # todo: generate better implementation than this
        while (num := random.randint(1, U32.MAX)) in self._channels:
            pass

        ack = self._loop.create_future()
        hdr = PacketHeader(PacketFlags.SYN, U32(num))
        hdr = self._send(hdr, service.encode("UTF-8"))

        self._ack_handler[hdr.channel, hdr.sequence] = ack
        self._logger.debug("open_channel: waiting for ACK")
        await ack

        ack_hdr, _ = ack.result()
        if ack_hdr.classify() is not ChannelPacketType.OPEN_CHANNEL:
            raise RuntimeError("invalid response to open_channel")

        self._logger.debug("open_channel: opened")
        self._channels[num] = Channel(num, self)

        return self._channels[num]

    async def close_channel(self, channel: Channel):
        """
        Close a channel for a particular service.

        Must be connected.

        :param channel: channel to close.
        """
        self._ensure_channel_connected(channel)

        self._logger.debug(f"close_channel: closing {channel}")

        num = channel.number
        ack = self._loop.create_future()
        hdr = self._send(PacketHeader(PacketFlags.FIN, U32(num)))

        self._ack_handler[hdr.channel, hdr.sequence] = ack
        self._logger.debug("close_channel: waiting for ACK")
        await ack

        ack_hdr, _ = ack.result()
        if ack_hdr.classify() is not ChannelPacketType.CLOSE_CHANNEL:
            raise RuntimeError("invalid response to close_channel")

        self._logger.debug("close_channel: closed")
        self._handle_channel_reset(hdr)

    def reset_channel(self, num: int):
        """
        Reset a channel.

        :param num: channel number. cannot be zero.
        """
        if not num:
            raise ValueError("cannot close channel zero")

        hdr = self._send(PacketHeader(PacketFlags.RST, U32(num)))
        self._handle_channel_reset(hdr)


class Session(asyncio.DatagramProtocol):
    """
    IPv4 implementation of the CAL session protocol.
    """
    def __init__(self):
        self._endpoints: dict[Endpoint.Address, Endpoint] = {}

    def _send(self, addr: Endpoint.Address, data: bytes):
        """
        Send raw data over the transport this protocol uses.

        :param addr: address to send to.
        :param data: data to send.
        """
        self._transport.sendto(data, addr)

    def connection_made(self, transport: transports.DatagramTransport):
        # noinspection PyAttributeOutsideInit
        self._transport = transport

    def datagram_received(self, data: bytes, addr: Endpoint.Address):
        endpoint = self.get_or_create_endpoint(addr)
        # noinspection PyProtectedMember
        asyncio.create_task(endpoint._receive(data))

    def get_or_create_endpoint(self, addr: Endpoint.Address):
        return self._endpoints.setdefault(addr, Endpoint(addr, self))
