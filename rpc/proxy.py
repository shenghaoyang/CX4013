"""
proxy.py

RPC proxy generator.
"""


import inspect
import asyncio
from boltons.funcutils import FunctionBuilder
from abc import ABC
from serialization.common import Serializable
from rpc.exceptions import ArgumentSerializationError, ReturnDeserializationError
from rpc.common import RemoteMethod, is_remotemethod, RemoteInterface
from rpc.protocol import RPCClient
from rpc.packet import InvocationSemantics
from typing import Optional, Type, Callable, cast


class ProxyController:
    def __init__(self):
        pass


class Proxy(ABC):
    """
    Abstract class used to designate a proxy.
    """

    def __init__(self, client: RPCClient):
        # Default uses at least once semantics
        self._client = client
        self._semantics = InvocationSemantics.AT_LEAST_ONCE
        self._timeout: Optional[float] = None
        self._retries: int = 0

    def set_semantics(self, semantics: InvocationSemantics):
        self._semantics = semantics

    def set_timings(self, timeout: Optional[float] = None, retries: Optional[int] = 0):
        """
        Set the RPC invocation timings.

        :param timeout: RPC invocation timeout. ``None`` for no timeout.
        :param retries: number of retries to make within timeout. Cannot be > ``0``
            if timeout is ``None``.
        """
        if timeout is not None and timeout < 0:
            raise ValueError(f"negative timeout")

        if retries < 0:
            raise ValueError(f"negative retries")

        if retries > 0 and timeout is None:
            raise ValueError(f"retries nonzero when timeout is None")

        self._timeout = timeout
        self._retries = retries

    async def _call(self, ordinal: int, arguments: bytes) -> bytes:
        """
        Implement the RPC call with timeouts and retries.

        :param ordinal: ordinal of remote method to call.
        :param arguments: serialized arguments.
        :return: serialized data.
        :raises asyncio.TimeoutError: on timeout.
        """
        timeout = (
            self._timeout if not self._retries else (self._timeout / self._retries)
        )
        tries = self._retries + 1
        for i in range(tries):
            try:
                return await asyncio.wait_for(
                    self._client.call(ordinal, arguments, self._semantics), timeout
                )
            except asyncio.TimeoutError:
                continue
        else:
            raise asyncio.TimeoutError


def proxy(remote_method: RemoteMethod, ordinal: int) -> Callable:
    """
    Generate a method proxying a remote method.

    The method will have the same signature as the remote method.

    However, its body will consist of a call to the ``Proxy`` class ``_call()`` method  to forward
    the method call across the network.

    :param remote_method: Remote method to proxy.
    :param ordinal: ordinal of remote method in class.
    """
    signature = inspect.signature(remote_method)

    # check that all parameters are positional or keyword and are serializable
    if not issubclass(signature.return_annotation, Serializable):
        raise TypeError(
            f"return type {signature.return_annotation} is not serializable"
        )

    returntype = signature.return_annotation
    # skip "self"
    argnames: list[str] = list(signature.parameters)[1:]

    async def body(self: Proxy, *args: Serializable) -> returntype:
        arguments = bytearray()
        for i, arg in enumerate(args):
            try:
                arguments.extend(arg.serialize())
            except Exception as e:
                raise ArgumentSerializationError(f"argument {i}: {arg}") from e

        ret = await self._call(ordinal, arguments)

        try:
            return returntype.deserialize(ret)
        except Exception as e:
            raise ReturnDeserializationError(f"of type {returntype}") from e

    builder = FunctionBuilder.from_func(remote_method)
    builder.is_async = True
    builder.body = f"return await body(self, {','.join(argnames)})"

    return builder.get_func(execdict={"body": body})


def generate_proxy(cls: Type[RemoteInterface]) -> Type[Proxy]:
    """
    Generate a proxy for a remote interface.

    Serialization and deserialization will happen automatically.

    :param cls: interface to generate a proxy for.
    """
    if not issubclass(cls, RemoteInterface):
        raise TypeError(f"{cls} is not a remote interface")

    remote_methods = inspect.getmembers(
        cls, lambda v: inspect.isfunction(v) and is_remotemethod(v)
    )
    proxy_methods: dict[str, Callable] = {}
    for i, (name, method) in enumerate(remote_methods):
        method = cast(RemoteMethod, method)
        proxied = proxy(method, i)
        proxy_methods[name] = proxied

    return cast(Type[Proxy], type(f"{cls.__name__}Proxy", (Proxy,), proxy_methods))
