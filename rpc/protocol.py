"""
protocol.py

RPC protocol handler implementations.
"""


import time
import asyncio
import random
from asyncio import DatagramProtocol, transports
from typing import Optional, TypeVar, Hashable, Callable, Iterator, Union
from collections.abc import MutableMapping, Coroutine
from serialization.numeric import u32
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


# We don't care what type it is so long as it is hashable.
AddressType = TypeVar("AddressType", bound=Hashable)
# The full client identifier consists of the client's network address + 32-bit client identifier.
FullClientIdentifier = tuple[AddressType, int]


class ResultCache(MutableMapping[int, bytes]):
    """
    Class representing the RPC result cache.
    """

    def __init__(self, lifetime: float = 3600.0):
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

    def __init__(self, skel: Skeleton, lifetime: float = 3600):
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


class TaskManager:
    """
    Task manager that watches over work done by multiple clients.
    """

    def __init__(self):
        # todo could add task count limit as well as task time limit.
        self._tasks: dict[AddressType, set[asyncio.Task]] = {}

    def _remove_task(self, addr: AddressType, task: asyncio.Task = None):
        """
        Remove a task from the manager.

        The task will be cancelled to avoid resource leaks.

        This method silently ignores clients without tasks, or tasks that
        are not managed by this manager. All tasks passed to this method
        will be cancelled regardless.

        :param addr: address of client.
        :param task: task to remove. Use ``None`` to remove all tasks.
        """
        if addr not in self._tasks:
            return

        tset = self._tasks[addr]
        if task is None:
            for t in tset:
                t.cancel()
            self._tasks[addr] = set()
            return

        task.cancel()
        if task in tset:
            tset.remove(task)

        if not len(tset):
            del self._tasks[addr]

    def create_task(self, addr: AddressType, coro: Coroutine) -> asyncio.Task:
        """
        Create a task to do work requested by a client.

        :param addr: client address.
        :param coro: coroutine to execute.
        :return: created task.
        """
        tsk = asyncio.create_task(coro)
        tsk.add_done_callback(lambda t: self._remove_task(addr, t))
        self._tasks.setdefault(addr, set()).add(tsk)

        return tsk

    def cancel_tasks(self, addr: AddressType = None):
        """
        Cancel all tasks for a given client.

        It's safe to cancel tasks for a client that has none.

        :param addr: client address. Use ``None`` to cancel for all clients.
        """
        addrs = iter(self._tasks) if addr is None else (addr,)
        for a in addrs:
            self._remove_task(a)


class RPCServer(DatagramProtocol):
    def __init__(
        self,
        skel_fac: Callable[[AddressType], Skeleton],
        disconnect_callback: Callable[[AddressType, Skeleton], None],
    ):
        """
        Create a new RPC server.

        :param skel_fac: skeleton factory generating skeletons that this server
            sends received RPC requests to.
        :param disconnect_callback: callback that will be invoked on client disconnections.
        """
        # todo figure out how to age out clients
        self._skel_fac = skel_fac
        self._disconnect_callback = disconnect_callback
        # maps addresses to cid, server instance.
        self._clients: dict[AddressType, tuple[int, RPCObjectServer]] = {}
        self._tasks = TaskManager()
        self._transport: Optional[transports.DatagramTransport] = None

    def connection_made(self, transport: transports.DatagramTransport) -> None:
        self._transport = transport

    def _send_rst(self, to: AddressType):
        """
        Send a RST datagram.

        :param to: address of target.
        """
        hdr = PacketHeader(flags=PacketFlags.RST | PacketFlags.REPLY)
        self._transport.sendto(hdr.serialize(), to)

    def _disconnect_client(self, client: AddressType = None, send_rst: bool = True):
        """
        Disconnect a client.

        :param client: address of client to disconnect. Use ``None`` to disconnect all clients.
        :param send_rst: whether to send reset to client.
        """

        def do_disconnect(addr: AddressType, oserver: RPCObjectServer):
            if send_rst:
                self._send_rst(addr)

            self._disconnect_callback(addr, oserver.skeleton)
            self._tasks.cancel_tasks(addr)

        if client is None:
            for addr, (_, oserver) in self._clients.items():
                do_disconnect(addr, oserver)

            self._clients = {}
            return

        _, oserver = self._clients[client]
        do_disconnect(client, oserver)
        del self._clients[client]

    async def _process_task(
        self,
        oserver: RPCObjectServer,
        hdr: PacketHeader,
        payload: bytes,
        addr: AddressType,
    ):
        """
        Task that does RPC work for the incoming datagram.

        See ``datagram_received()`` for more information on the parameters.
        """
        ret = await oserver.process(hdr, payload)
        if ret is None:
            return

        self._transport.sendto(ret, addr)
        return

    def disconnect_client(self, client: AddressType = None):
        """
        Disconnect a client.

        :param client: address of client to disconnect. Use ``None`` to disconnect all clients.
        """
        self._disconnect_client(client)

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

        cid = hdr.client_id.value

        # Handle RST
        if hdr.flags & PacketFlags.RST:
            if addr in self._clients:
                self._disconnect_client(addr, False)
            return

        if not cid:
            # New connection or RST.
            # At this point we have validated that the client has an open connection with
            # this server.
            if addr in self._clients:
                # Stale connection exists for the same network address.
                # Drop.
                self.disconnect_client(addr)

            oserver = RPCObjectServer(self._skel_fac(addr))
            # Generate random CID to avoid collisions with previously connected clients.
            cid = random.randint(u32.min() + 1, u32.max())
            self._clients[addr] = cid, oserver

            # register and send new CID
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

        scid, oserver = self._clients[addr]
        if scid != cid:
            # Unknown client, also one that thinks it's using an open connection
            # but server also doesn't have a record of that connection. Reset.
            # todo: need to notify servers of shutdown!
            self.disconnect_client(addr)
            return

        # At this point we have validated that the client has an open connection with
        # this server.
        # Perform RPC work on behalf of client.
        self._tasks.create_task(addr, self._process_task(oserver, hdr, payload, addr))


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
    def __init__(self, peer_adr: AddressType):
        """
        Create a new RPC client.

        :param peer_adr: address to connect to.
        """
        self._txid = TransactionID.random()
        self._cid: int = 0
        self._peer = peer_adr
        self._closed = False
        self._router = ReplyRouter()
        self._connected_event = asyncio.Event()
        self._transport: Optional[transports.DatagramTransport] = None

    def connection_made(self, transport: transports.DatagramTransport):
        self._transport = transport
        # Obtain an ID.
        self._transport.sendto(PacketHeader().serialize(), self._peer)

    def __bool__(self) -> bool:
        """
        Returns ``True`` if the client can be used for RPC calls.
        """
        return self._connected_event.is_set()

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
            return

        if hdr.flags & PacketFlags.RST:
            self._close(False)
            return

        if hdr.client_id.value != self._cid:
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
            self._transport.sendto(
                PacketHeader(flags=PacketFlags.RST).serialize(), self._peer
            )

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
        # todo we want to propagate any "resend" info
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

            self._transport.sendto(hdr.serialize() + args, self._peer)
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
                self._transport.sendto(hdr.serialize(), self._peer)

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
