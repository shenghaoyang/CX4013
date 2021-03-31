"""
proxy.py

RPC proxy generator.
"""


import inspect
from abc import ABC
from typing import Type, Callable, cast

from boltons.funcutils import FunctionBuilder

from cx4013.rpc.common import RemoteMethod, is_remotemethod, RemoteInterface
from cx4013.rpc.exceptions import ArgumentSerializationError, ReturnDeserializationError
from cx4013.rpc.protocol import RPCClient
from cx4013.serialization.common import Serializable


class Proxy(ABC):
    """
    Abstract class used to designate a proxy.
    """

    def __init__(self, client: RPCClient):
        # Default uses at least once semantics
        self._client = client


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

        ret = await self._client.call(ordinal, arguments)

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
