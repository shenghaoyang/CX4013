"""
skeleton.py

RPC Skeleton generator.
"""


import inspect
from rpc.exceptions import (
    ArgumentDeserializationError,
    InternalServerFailureError,
    MethodNotFoundError,
)
from rpc.common import RemoteMethod, RemoteInterface, is_remotemethod
from serialization.common import Serializable
from typing import Type, cast, Union, Callable
from collections import Awaitable

# Signature of a method defined on an instance of a Skeleton class.
SkeletonMethod = Callable[["Skeleton", bytes], Awaitable[bytes]]


class Skeleton:
    """
    Class used to tag a class as a skeleton.
    """

    # Used for type checking, isn't actually empty in most generated skeletons.
    _method_list: list[SkeletonMethod] = []

    def __init__(self, impl: RemoteInterface):
        """
        Create a new skeleton delegating calls to implementation ``impl``.

        :param impl: Concrete implementation instance to delegate to.
        """
        self._impl = impl

    async def _call_by_ordinal(self, ordinal: int, arguments: bytes) -> bytes:
        """
        Call a method by specifying its ordinal.

        :param ordinal: method ordinal.
        :param arguments: method arguments.
        :return: invocation result.
        """
        if not (0 <= ordinal < len(self._method_list)):
            raise MethodNotFoundError(ordinal)

        return await self._method_list[ordinal](self, arguments)


def wrap(
    remote_method: RemoteMethod
) -> SkeletonMethod:
    """
    Generate a method wrapping a remote method.

    The function will deserialize a ``bytes`` argument and call the
    wrapped implementation method with the deserialized arguments.

    It will also serialize the return value back to a ``bytes`` value.

    :param remote_method: Remote method to serialize.
    """
    signature = inspect.signature(remote_method)

    # check that all parameters are positional or keyword and are serializable
    if not issubclass(signature.return_annotation, Serializable):
        raise TypeError(
            f"return type {signature.return_annotation} is not serializable"
        )

    argtypes = []
    returntype = signature.return_annotation
    for i, (name, parameter) in enumerate(signature.parameters.items()):
        if not i:
            # skip "self"
            continue

        if name.startswith("_"):
            # todo: maybe check during the decorator?
            raise ValueError(f"remote method {name} cannot start with _")

        if parameter.kind != inspect.Parameter.POSITIONAL_OR_KEYWORD:
            raise TypeError(
                f"parameter {name} of {remote_method} is not of "
                f"positional or keyword type"
            )

        if not issubclass(parameter.annotation, Serializable):
            raise TypeError(f"parameter {name} of {remote_method} is not serializable")
        argtypes.append(parameter.annotation)

    # wrapper function that's actually bound as a method of the skeleton class.
    # closures are so *nice*.
    async def wrapper(self: Skeleton, serialized_arguments: bytes) -> bytes:
        off = 0
        arguments = []
        for i, argtype in enumerate(argtypes):
            try:
                arguments.append(argtype.deserialize(serialized_arguments[off:]))
            except Exception as e:
                raise ArgumentDeserializationError(
                    f"argument {i} of type {argtype}"
                ) from e
            off += arguments[-1].size

        ret = await remote_method(self._impl, *arguments)

        if type(ret) is not returntype:
            raise InternalServerFailureError(
                f"return type {type(ret)} mismatch (expected {returntype})"
            )
        try:
            serialized_ret = ret.serialize()
        except Exception as e:
            raise InternalServerFailureError(
                f"return value of type {returntype}"
            ) from e

        return serialized_ret

    return wrapper


def generate_skeleton(cls: Type[RemoteInterface]) -> Type[Skeleton]:
    """
    Generate a skeleton for a remote interface.

    These skeletons can be instantiated with an instance of a class implementing the
    remote interface, like:

    ``skel = Skeleton(impl)``.

    Arguments will be transparently serialized & deserialized for calls into
    remote methods of the concrete class.

    :param cls: interface to generate a skeleton for.
    """
    if not issubclass(cls, RemoteInterface):
        raise TypeError(f"{cls} is not a remote interface")

    remote_methods = inspect.getmembers(
        cls, lambda v: inspect.isfunction(v) and is_remotemethod(v)
    )
    skeleton_methods: dict[str, Union[SkeletonMethod, list[SkeletonMethod]]] = {}
    method_list: list[SkeletonMethod] = []
    for name, method in remote_methods:
        wrapped = wrap(method)
        skeleton_methods[name] = wrapped
        method_list.append(wrapped)

    skeleton_methods["_method_list"] = method_list

    return cast(
        Type[Skeleton], type(f"{cls.__name__}Skeleton", (Skeleton,), skeleton_methods)
    )
