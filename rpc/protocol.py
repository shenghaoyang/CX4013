"""
protocol.py

RPC protocol handler implementations.
"""


import time
import asyncio
import random
from asyncio import DatagramProtocol, transports
from typing import Optional, TypeVar, Hashable, Callable, Iterator, Union
from collections.abc import MutableMapping, Coroutine, Mapping
from serialization.numeric import u32
from functools import partial
from rpc import exceptions
from rpc.skeleton import Skeleton
from rpc.packet import (
    PacketHeader,
    PacketFlags,
    ExecutionStatus,
    InvocationSemantics,
    exception_to_estatus,
    TransactionID,
    estatus_to_exception,
)


# Signature of a pre-receive / pre-send hook.
# Accepts packet to be sent / packet received and returns transformed packet, or
# ``None`` to drop the packet.
HookCallable = Callable[[bytes], Optional[bytes]]
# We don't care what type it is so long as it is hashable.
AddressType = TypeVar("AddressType", bound=Hashable)
# The full client identifier consists of the client's network address + 32-bit client identifier.
FullClientIdentifier = tuple[AddressType, int]


class ResultCache(MutableMapping[int, bytes]):
    """
    Class representing the RPC result cache.
    """

    def __init__(self, lifetime: int = 60):
        """
        Create a new RPC result cache.

        :param lifetime: maximum lifetime of entries in the result cache (seconds).
        """
        # maps transaction ID -> result, insertion time
        # can be made more efficient but we focus on that later
        # e.g. list ordered by insertion time, etc.
        self._lifetime = lifetime
        self._cache: dict[int, tuple[Union[bytes, Exception], float]] = {}

    def __setitem__(self, tid: int, result: bytes):
        """
        Insert a new entry into the RPC result cache.

        :param tid: transaction ID.
        :param result: serialized result of executing the method.
        """
        if tid in self:
            raise KeyError(f"transaction {tid} already cached")

        self._cache[tid] = (result, time.monotonic())

    def __getitem__(self, tid: int) -> Union[bytes, Exception]:
        """
        Retrieve a cached result.

        :param tid: transaction ID.
        :return: cached result.
        """
        return self._cache[tid][0]

    def __delitem__(self, tid: int):
        """
        Delete a cached result.

        :param tid: transaction ID associated with result.
        """
        del self._cache[tid]

    def __iter__(self) -> Iterator[int]:
        """
        Obtain an iterator iterating over the transaction IDs of transactions
        whose results have been cached.
        """
        return iter(self._cache)

    def __len__(self) -> int:
        """
        Obtain the number of cached method calls.
        """
        return len(self._cache)

    def ageout(self) -> int:
        """
        Delete entries in the cache that have exceeded their life time.

        :return: number of entries aged out.
        """
        now = time.monotonic()
        delete = [
            k
            for k, (_, insertion_time) in self._cache.items()
            if (now - insertion_time) > self._lifetime
        ]
        for k in delete:
            del self[k]

        return len(delete)

    def time_to_next_ageout(self) -> Optional[float]:
        """
        Obtain the time from now to the age out time for the oldest entry in the cache.

        The result value is positive if the age out time is in the future, and negative if it is in the
        past.

        :return: time delta in seconds. ``None`` if there are no entries.
        """
        if not len(self):
            return None

        insertion_time_earliest = min(map(lambda t: t[1], self.values()))
        age_out_time = insertion_time_earliest + self._lifetime
        delta = age_out_time - time.monotonic()

        return delta


class RPCObjectServer:
    """
    Class representing an object server providing access to a single remote object.
    """

    def __init__(self, skel: Skeleton, lifetime: int = 60):
        """
        Create a new object server.

        :param skel: object skeleton.
        :param lifetime: time in seconds to cache results (for at-most-once invocations).
        """
        self._skel = skel
        self._result_cache = ResultCache(lifetime)

    @staticmethod
    def reply_packet(
        orig_hdr: PacketHeader,
        flags: PacketFlags,
        status: ExecutionStatus,
        ret: bytes = b"",
    ) -> bytes:
        """
        Generate a reply packet with an execution status and a method return value.

        :param orig_hdr: header of original request.
        :param flags: packet flags.
        :param status: RPC execution status.
        :param ret: serialized return value

        :return: packet data.
        """
        out = bytearray(
            PacketHeader(
                orig_hdr.client_id,
                orig_hdr.trans_num,
                flags,
                orig_hdr.semantics,
                orig_hdr.method_ordinal,
            ).serialize()
        )
        out.append(status.value)
        out.extend(ret)

        return out

    async def _call_alo(self, ordinal: int, args: bytes) -> bytes:
        """
        Call a method on the skeleton with at-least-once invocation semantics.

        :param ordinal: method ordinal.
        :param args: serialized method arguments.
        :return: serialized method results.
        """
        # noinspection PyProtectedMember
        return await self._skel._call_by_ordinal(ordinal, args)

    async def _call_amo(
        self, tid: int, ordinal: int, args: bytes
    ) -> [Union[bytes, Exception], bool]:
        """
        Call a method on the skeleton with at-most-once invocation semantics.

        :param tid: transaction ID.
        :param ordinal: method ordinal.
        :param args: serialized method arguments.
        :return: tuple (method results, result retrieved from cache)
        """
        if tid in self._result_cache:
            res = self._result_cache[tid]
            return res, True

        res = await self._call_alo(ordinal, args)
        self._result_cache[tid] = res

        return res, False

    @property
    def skeleton(self) -> Skeleton:
        """
        Retrieve the skeleton used by this server.
        """
        return self._skel

    async def process(self, hdr: PacketHeader, payload: bytes) -> Optional[bytes]:
        """
        Process a single RPC request packet.

        :param hdr: packet header.
        :param payload: packet payload.
        :return: reply packet. ``None`` if no reply can be generated.
        """
        # todo scheduled task may be better
        self._result_cache.ageout()

        args = payload

        # Bad header
        if hdr.is_reply:
            return RPCObjectServer.reply_packet(
                PacketFlags.REPLY, ExecutionStatus.BAD_REQUEST
            )

        # Client is ACKing a previous reply.
        # Drop cached reply, if it exists.
        # We don't care about the invocation semantics because it doesn't really matter.
        if hdr.flags & PacketFlags.ACK_REPLY:
            tid = hdr.trans_num.value
            self._result_cache.pop(tid, None)
            return

        try:
            if hdr.semantics is InvocationSemantics.AT_LEAST_ONCE:
                res = await self._call_alo(hdr.method_ordinal.value, args)
                return RPCObjectServer.reply_packet(
                    hdr, PacketFlags.REPLY, ExecutionStatus.OK, res
                )
            else:
                res, cached = await self._call_amo(
                    hdr.trans_num.value, hdr.method_ordinal.value, args
                )

                status = ExecutionStatus.OK
                if isinstance(res, Exception):
                    if isinstance(res, exceptions.RPCError):
                        status = exception_to_estatus(type(res))
                    else:
                        status = ExecutionStatus.INTERNAL_FAILURE
                    res = b""

                return RPCObjectServer.reply_packet(
                    hdr,
                    PacketFlags.REPLY
                    | (PacketFlags.REPLAYED if cached else PacketFlags.NONE),
                    status,
                    res,
                )
        except exceptions.RPCError as e:
            status = exception_to_estatus(type(e))
        except Exception:
            status = ExecutionStatus.INTERNAL_FAILURE

        return RPCObjectServer.reply_packet(hdr, PacketFlags.REPLY, status)


class ConnectedClient:
    """
    Class that represents a connected client.

    Used at the server-side to manage clients.
    """

    def __init__(
        self,
        caddr: AddressType,
        transport: transports.DatagramTransport,
        skel: Skeleton,
        timeout_callback: Callable[[AddressType], None],
        inactivity_timeout: int = 300,
        result_cache_timeout: int = 60,
    ):
        """
        Create a new connected client.

        :param caddr: client address.
        :param transport: transport used for communication with the client.
        :param skel: bound skeleton used when handling client requests.
        :param timeout_callback: callback invoked when this client times out.
            The `ConnectedClient` would be in a disconnected state when this callback
            is invoked. Use ``None`` to specify that no callback should be invoked.
        :param inactivity_timeout: time (in seconds) without receiving packets from the client
            before a client is disconnected.
        :param result_cache_timeout: time (in seconds) to cache the result of a RPC call
            invoked using at-most-once semantics.
        """
        self._loop = asyncio.get_running_loop()
        self._caddr = caddr
        self._transport = transport
        self._timeout_callback = timeout_callback
        self._oserver = RPCObjectServer(skel, result_cache_timeout)

        # Needs to manage.
        # Maps transaction numbers to tasks spawned for handling them.
        self._tmgr = TaskManager(self._task_done)
        self._disconnected = False
        # Client sequence number.
        # Last activity time: time of last packet activity (send / receive).
        self._last_activity_time = time.monotonic()
        self._inactivity_check_task = asyncio.create_task(
            self._inactivity_check_loop(inactivity_timeout)
        )

    def __bool__(self) -> bool:
        """
        Obtain the connection state.

        :return: `False` for disconnected, `True` for connected.
        """
        return not self._disconnected

    async def _inactivity_check_loop(self, after: float):
        """
        Loop that will disconnect the client by checking if the last
        activity time (after sleeping for a certain time) matches the before-sleep last activity time.

        Used to implement inactivity monitoring.

        :param after: time to sleep (in seconds).
        """
        while True:
            saved = self._last_activity_time
            await asyncio.sleep(after)
            inactive = self._last_activity_time == saved

            if inactive:
                # Do nothing if already disconnected.
                if not self:
                    return

                self.disconnect()
                self._timeout_callback(self._caddr)
                return

    def _send_packet(self, packet: bytes):
        """
        Send a packet to the client.

        :param packet: packet to send.
        """
        self._transport.sendto(packet, self._caddr)

    def _task_done(self, txid: int, tsk: asyncio.Task):
        """
        Handle a task that was done.

        :param txid: transaction ID of completed task.
        :param tsk: completed task.
        """
        # Discard all tasks completed after disconnection.
        if not self:
            return

        # RPC object server should not raise exceptions.
        assert tsk.exception() is None

        packet = tsk.result()
        if packet is not None:
            self._send_packet(packet)

    def process(self, hdr: PacketHeader, payload: bytes):
        """
        Process a packet received from the client.

        :param hdr: packet header.
        :param payload: packet payload.
        """
        self._last_activity_time = time.monotonic()
        txid = hdr.trans_num.value

        if hdr.flags & PacketFlags.PING:
            hdr.flags |= PacketFlags.REPLY
            self._send_packet(hdr.serialize())
            return

        # Ignore requests with txids corresponding to executing tasks.
        if txid in self._tmgr:
            return

        # Otherwise, create task to run oserver.
        self._tmgr.create_task(txid, self._oserver.process(hdr, payload))

    def disconnect(self):
        """
        Disconnect this client.
        """
        if not self:
            return

        self._tmgr.cancel_tasks()
        self._inactivity_check_task.cancel()
        self._disconnected = True
        # todo: implement oserver stop
        # self._oserver.stop()


class TaskManager(Mapping[int, asyncio.Task]):
    """
    Task manager managing tasks spawned for a connected client.
    """

    def __init__(self, done_callback: Callable[[int, asyncio.Task], None]):
        """
        Create a new task manager.

        :param done_callback: callback to invoke when a task is done.
            Will be called using ``loop.call_soon()``.
            Note that this will be invoked even if the task raised an exception.
            See the asyncio documentation for the definition of "done" with respect
            to a task.
        """
        # Dictionary mapping transaction IDs to tasks spawned for those IDs.
        self._tasks: dict[int, asyncio.Task] = {}
        self._done_callback = done_callback

    def __getitem__(self, txid: int) -> asyncio.Task:
        """
        Retrieve the task associated with a transaction ID.

        :param txid: transaction ID to lookup.
        """
        return self._tasks[txid]

    def __iter__(self) -> Iterator[int]:
        """
        Return an iterator yielding transaction IDs with active tasks.
        """
        return iter(self._tasks)

    def __len__(self) -> int:
        """
        Obtain the number of active tasks.
        """
        return len(self._tasks)

    def _task_done(self, txid: int, tsk: asyncio.Task):
        del self._tasks[txid]
        self._done_callback(txid, tsk)

    def create_task(self, txid: int, coro: Coroutine) -> asyncio.Task:
        """
        Create a task to do work requested by a client.

        :param txid: transaction id.
        :param coro: coroutine to execute.
        :return: created task.
        """
        tsk = asyncio.create_task(coro)
        tsk.set_name(f"{self.__class__.__name__}: task for txid {txid}")
        tsk.add_done_callback(partial(self._task_done, txid))
        self._tasks[txid] = tsk

        return tsk

    def cancel_tasks(self):
        """
        Cancel all tasks.

        Cancelled tasks will be removed.
        """
        # tasks will be removed by the done callback.
        for t in self._tasks.values():
            t.cancel()


class RPCServer(DatagramProtocol):
    def __init__(
        self,
        skel_fac: Callable[[AddressType], Skeleton],
        disconnect_callback: Callable[[AddressType], None],
        inactivity_timeout: int = 300,
        result_cache_timeout: int = 60,
    ):
        """
        Create a new RPC server.

        :param skel_fac: skeleton factory generating skeletons that this server
            sends received RPC requests to.
        :param disconnect_callback: callback that will be invoked on client disconnections.
            May be called immediately on disconnections. No guarantees as to whether
            ``loop.call_soon()`` will be used.
        :param inactivity_timeout: amount of time (in seconds) without receiving a request
            before a client is disconnected.
        :param result_cache_timeout: amount of time (in seconds) to cache the result of an
            RPC call made using at-most-once semantics.
        """
        # todo figure out how to age out clients
        self._skel_fac = skel_fac
        self._disconnect_callback = disconnect_callback
        self._inactivity_timeout = inactivity_timeout
        self._result_cache_timeout = result_cache_timeout

        # maps addresses to cid, server instance.
        self._clients: dict[AddressType, tuple[int, ConnectedClient]] = {}
        self._transport: Optional[transports.DatagramTransport] = None
        self._stopped = False

    def connection_made(self, transport: transports.DatagramTransport):
        self._transport = transport

    def __bool__(self):
        """
        Check whether the server is running.
        """
        return not self._stopped

    def _send_rst(self, to: AddressType):
        """
        Send a RST datagram.

        :param to: address of target.
        """
        hdr = PacketHeader(flags=PacketFlags.RST | PacketFlags.REPLY)
        self._transport.sendto(hdr.serialize(), to)

    def _disconnect_clients(self):
        """
        Disconnect all clients.

        RST packets will be sent to all clients unconditionally.
        """
        # This construction is needed to avoid mutating the dictionary
        # while iterating through it.
        while len(self._clients):
            caddr = next(iter(self._clients))
            self.disconnect_client(caddr)

    def disconnect_client(self, client: AddressType, send_rst: bool = True):
        """
        Disconnect a client.

        :param client: address of client to disconnect.
        :param send_rst: whether to send a RST packet to the client.
        """
        self._clients[client][1].disconnect()
        if send_rst:
            self._send_rst(client)

        del self._clients[client]
        self._disconnect_callback(client)

    def datagram_received(self, data: bytes, addr: AddressType):
        """
        Process an incoming datagram.

        :param data: data received.
        :param addr: datagram source address.
        """
        try:
            hdr = PacketHeader.deserialize(data)
            payload = data[hdr.LENGTH :]
        except ValueError:
            # Nothing we can do, packet cannot be decoded.
            return

        # Filter replies
        if hdr.is_reply:
            return

        # Check for an RST.
        if hdr.flags & PacketFlags.RST:
            # Disconnect active client.
            if addr in self._clients:
                self.disconnect_client(addr, False)
            return

        # Non RST, continuation of previous session or new connection.
        cid = hdr.client_id.value

        # New connection.
        if not cid:
            if addr in self._clients:
                # Delete stale connection for the same network address.
                self.disconnect_client(addr, False)

            skel = self._skel_fac(addr)
            # Generate random CID to avoid collisions with previously connected clients.
            cid = random.randint(u32.min() + 1, u32.max())
            self._clients[addr] = cid, ConnectedClient(
                caddr=addr,
                skel=skel,
                transport=self._transport,
                timeout_callback=self.disconnect_client,
                inactivity_timeout=self._inactivity_timeout,
                result_cache_timeout=self._result_cache_timeout,
            )

            # Register and send new CID
            rep = PacketHeader(
                client_id=u32(cid), flags=(PacketFlags.REPLY | PacketFlags.CHANGE_CID)
            )
            self._transport.sendto(rep.serialize(), addr)
            return

        # Existing connection
        if addr not in self._clients:
            # Unknown client
            # Client thinks it's using an open connection but server
            # has no record of that connection. Reset to signal that.
            self._send_rst(addr)
            return

        scid, cclient = self._clients[addr]
        if scid != cid:
            # Unknown client, also one that thinks it's using an open connection
            # but server also doesn't have a record of that connection. Reset.
            # todo: need to notify servers of shutdown!
            self.disconnect_client(addr)
            return

        cclient.process(hdr, payload)

    def stop(self):
        """
        Stop the RPC server.

        No-op if already stopped.
        """
        if not self:
            return

        self._disconnect_clients()
        self._transport.close()
        self._stopped = True


class ReplyRouter:
    """
    Class that's used for the ``RPCClient`` for routing replies by their transaction ID.
    """

    def __init__(self):
        self._loop = asyncio.get_running_loop()
        self._listeners: dict[int, asyncio.Future] = {}

    def _remove_listeners(self, txid: int = None):
        """
        Remove listeners waiting on a transaction.

        Associated futures will be cancelled to avoid resource leaks.

        :param txid: transaction id. ``None`` targets all listeners.
        """
        if txid is None:
            for f in self._listeners.values():
                f.cancel()
            self._listeners = {}
            return

        self._listeners[txid].cancel()
        del self._listeners[txid]

    def raise_on_listeners(self, e: Exception, txid: int = None):
        """
        Raise an exception on listeners waiting for a given transaction id.

        :param e: exception to raise.
        :param txid: transaction id. ``None`` targets all listeners.
        """
        if txid is None:
            for f in self._listeners.values():
                f.set_exception(e)
            return

        self._listeners[txid].set_exception(e)

    def route(self, hdr: PacketHeader, payload: bytes) -> bool:
        """
        Route a packet to the listener waiting for it.

        There may be no listener for a packet. In that case, the packet is
        discarded.

        :param hdr: packet header.
        :param payload: packet payload.
        :return: whether the packet was routed to its listener.
        """
        txid = hdr.trans_num.value
        if txid not in self._listeners:
            return False

        self._listeners[txid].set_result((hdr, payload))
        return True

    async def listen(self, txid: int) -> tuple[PacketHeader, bytes]:
        """
        Wait for a reply packet with a given transaction ID.

        :param txid: transaction ID of reply to wait for.
        :return: reply packet.
        """
        if txid in self._listeners:
            raise ValueError(f"{txid} has another listener")

        fut = self._loop.create_future()
        self._listeners[txid] = fut
        fut.add_done_callback(lambda _: self._remove_listeners(txid))

        return await fut


class RPCClient(DatagramProtocol):
    def __init__(
        self,
        peer_adr: AddressType,
        inactivity_timeout: int = 300,
        keepalive_interval: int = None,
        pre_receive: HookCallable = None,
        pre_send: HookCallable = None,
    ):
        """
        Create a new RPC client.

        :param peer_adr: address to connect to.
        :param inactivity_timeout: time (in seconds) without receiving any packets from the
            server before disconnecting from it.
        :param keepalive_interval: time (in seconds) between sending keepalive PING packets.
            Use ``None`` to infer from ``inactivity_timeout``.
        :param pre_receive: pre-receive hook. Called right after receiving any packet before any
            processing is done.
        :param pre_send: pre-send hook. Called right before sending any packet.
        """
        self._txid = TransactionID.random()
        self._cid: int = 0
        self._peer = peer_adr
        self._inactivity_timeout = inactivity_timeout
        if keepalive_interval is None:
            self._ping_interval = inactivity_timeout / 4
        else:
            self._ping_interval = keepalive_interval
        self._pre_receive = pre_receive
        self._pre_send = pre_send

        self._closed = False
        self._router = ReplyRouter()
        self._connected_event = asyncio.Event()
        self._transport: Optional[transports.DatagramTransport] = None
        self._last_activity_time = time.monotonic()
        self._inactivity_check_task: Optional[asyncio.Task] = None

    def connection_made(self, transport: transports.DatagramTransport):
        self._transport = transport
        # Obtain an ID.
        self._send(PacketHeader().serialize())

    def __bool__(self) -> bool:
        """
        Returns ``True`` if the client can be used for RPC calls.
        """
        return self._connected_event.is_set()

    def _send(self, data: bytes):
        """
        Send a packet to the server.
        """
        if self._pre_send is not None:
            data = self._pre_send(data)
            if data is None:
                return

        self._transport.sendto(data, self._peer)

    async def _ping_loop(self, interval: float):
        """
        Loop that will send PING packets to the server at a predefined interval.

        :param interval: interval to send ping packets at (in seconds).
        """
        while True:
            await asyncio.sleep(interval)
            self._send(
                PacketHeader(
                    client_id=u32(self._cid), flags=PacketFlags.PING
                ).serialize()
            )

    async def _inactivity_check_loop(self, after: float):
        """
        Loop that will disconnect the client by checking if the last
        activity time (after sleeping for a certain time) matches the before-sleep last activity time.

        Used to implement inactivity monitoring.

        :param after: time to sleep (in seconds).
        """
        while True:
            saved = self._last_activity_time
            await asyncio.sleep(after)
            inactive = self._last_activity_time == saved

            if inactive:
                # Do nothing if already disconnected.
                if not self:
                    return

                self.close()
                return

    async def _process_task(self, data: bytes):
        """
        Task that actually processes the incoming datagram.

        See ``datagram_received()`` for more information on the parameters.
        """
        try:
            hdr = PacketHeader.deserialize(data)
            payload = data[hdr.LENGTH :]
        except Exception:
            return

        if not hdr.is_reply:
            return

        if not self._connected_event.is_set():
            # todo do we want to ignore RSTs?
            if not (hdr.flags & PacketFlags.CHANGE_CID):
                return

            self._cid = hdr.client_id.value
            self._connected_event.set()
            self._last_activity_time = time.monotonic()
            self._ping_task = asyncio.create_task(self._ping_loop(self._ping_interval))
            self._inactivity_check_task = asyncio.create_task(
                self._inactivity_check_loop(self._inactivity_timeout)
            )
            return

        if hdr.flags & PacketFlags.RST:
            self._close(False)
            return

        if hdr.client_id.value != self._cid:
            return

        self._last_activity_time = time.monotonic()

        if hdr.flags & PacketFlags.PING:
            return

        self._router.route(hdr, payload)

    def _close(self, send_rst: bool = True):
        """
        Close the connection.

        Safe to call if already closed.

        :param send_rst: whether to send a RST packet.
        """
        if self.closed:
            return

        if send_rst:
            self._send(PacketHeader(flags=PacketFlags.RST).serialize())

        if (t := self._ping_task) is not None:
            t.cancel()
        if (t := self._inactivity_check_task) is not None:
            t.cancel()
        self._router.raise_on_listeners(exceptions.ConnectionClosedError())
        self._connected_event.clear()
        self._closed = True
        self._transport.close()

    @property
    def closed(self) -> bool:
        """
        Check if the connection has been closed.

        A connection cannot be reused if it has been closed.
        """
        return self._closed

    def datagram_received(self, data: bytes, addr: AddressType):
        if self.closed:
            return

        if addr != self._peer:
            return

        if self._pre_receive is not None:
            data = self._pre_send(data)
            if data is None:
                return

        asyncio.create_task(self._process_task(data))

    async def wait_connected(self):
        """
        Wait till the RPC client is connected.

        :raises exceptions.ConnectionClosedError: if the connection has been closed.
        """
        if self.closed:
            raise exceptions.ConnectionClosedError()

        await self._connected_event.wait()

    async def call(
        self,
        ordinal: int,
        args: bytes,
        semantics: InvocationSemantics = InvocationSemantics.AT_LEAST_ONCE,
        timeout: Optional[float] = None,
        retries: Optional[int] = 0,
    ) -> bytes:
        """
        Call a remote method.

        Will wait for a connection to be established if not already connected.

        :param ordinal: ordinal of the remote method.
        :param args: serialized arguments for the remote method.
        :param semantics: method invocation semantics.
        :param timeout: RPC invocation timeout. ``None`` for no timeout.
        :param retries: number of retries to make within timeout. Cannot be > ``0``
            if timeout is ``None``.
        :return: serialized return value for the remote method.
        :raises exceptions.RPCConnectionClosedError: if the connection has been closed.
        """
        # timeout & retries will be checked by set_semantics().
        if not self:
            await self.wait_connected()

        timeout = timeout if not timeout else (timeout / retries)
        tries = retries + 1

        # Generate initial header.
        tid = self._txid.next().copy()
        hdr = PacketHeader(
            u32(self._cid),
            trans_num=tid,
            semantics=semantics,
            method_ordinal=u32(ordinal),
        )

        for i in range(tries):
            if i:
                hdr.flags |= PacketFlags.REPLAYED

            self._send(hdr.serialize() + args)
            # shouldn't race because it's single threaded, and we don't hit an await
            # till the future gets submitted.
            try:
                rhdr, payload = await asyncio.wait_for(
                    self._router.listen(tid.value), timeout
                )
            except asyncio.TimeoutError:
                continue

            if not len(payload):
                raise exceptions.InvalidReplyError("zero-length payload")
            try:
                status = ExecutionStatus(payload[0])
            except ValueError:
                raise exceptions.InvalidReplyError("execution status")

            if semantics is InvocationSemantics.AT_MOST_ONCE:
                # send acknowledgement to optimize result storage
                # doesn't matter if it gets lost because the server will age it
                # out anyway.
                hdr.flags = PacketFlags.ACK_REPLY
                self._send(hdr.serialize())

            excc = estatus_to_exception(status)
            if excc is not None:
                raise excc()

            return payload[1:]
        else:
            raise asyncio.TimeoutError

    def close(self):
        """
        Close the connection.

        Safe to call if already closed.
        """
        self._close()
