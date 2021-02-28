"""
generators.py

RPC proxy and skeleton generators.
"""


import inspect
from typing import TypeVar, Callable, Any, Type, cast
from typing_extensions import Protocol
from rpc.exceptions import SerializationError, DeserializationError
from serialization.common import Serializable
from abc import ABC
from serialization.numeric import u32
from serialization.derived import create_struct_type, create_enum_type



C = TypeVar('C', bound=Callable)


class RemoteMethod(Protocol):
    def __call__(self, obj: Any, *args: Serializable) -> Serializable:
        pass


def remotemethod(func: C) -> C:
    func._calrpc_remote = True
    return func


def is_remotemethod(func: C) -> bool:
    return hasattr(func, '_calrpc_remote')


class RemoteInterface(ABC):
    """
    Class used to tag a class as a remote interface.

    Skeletons and proxies are generated using this remote interface.

    Define remote interfaces and implement them with concrete classes.
    """
    pass


class Calendar(RemoteInterface):
    def book_room(self, day: u32, date: u32) -> u32:
        pass


class Skeleton:
    """
    Class used to tag a class as a skeleton.
    """
    def __init__(self, impl: RemoteInterface):
        """
        Create a new skeleton delegating calls to implementation ``impl``.

        :param impl: Concrete implementation instance to delegate to.
        """
        self._impl = impl


SkeletonMethod = Callable[[Skeleton, bytes], bytes]


def generate_skeleton_wrap(remote_method: RemoteMethod, remote_class: Any) -> SkeletonMethod:
    """
    Generate a skeleton wrapping an implementation method.

    The function will deserialize a ``bytes`` argument and call the
    wrapped implementation method with the deserialized arguments.

    It will also serialize the return value back to a ``bytes`` value.

    :param remote_method: Remote method to serialize.
    :param remote_class: Class that method belongs to.
    """
    signature = inspect.signature(remote_method)

    # check that all parameters are positional or keyword and are serializable
    if not issubclass(signature.return_annotation, Serializable):
        raise TypeError(f"return type {signature.return_annotation} is not serializable")

    argtypes = []
    returntype = signature.return_annotation
    for i, (name, parameter) in enumerate(signature.parameters.items()):
        if not i:
            # skip "self"
            continue

        if parameter.kind != inspect.Parameter.POSITIONAL_OR_KEYWORD:
            raise TypeError(f"parameter {name} of {remote_method} is not of "
                            f"positional or keyword type")

        if not issubclass(parameter.annotation, Serializable):
            raise TypeError(f"parameter {name} of {remote_method} is not serializable")
        argtypes.append(parameter.annotation)

    def wrapper(self: Skeleton, serialized_arguments: bytes) -> bytes:
        off = 0
        arguments = []
        for i, argtype in enumerate(argtypes):
            try:
                arguments.append(argtype.deserialize(serialized_arguments[off:]))
            except Exception as e:
                raise DeserializationError(f"argument {i} of type {argtype}") from e
            off += arguments[-1].size

        ret = remote_method(self._impl, *arguments)

        if type(ret) is not returntype:
            raise SerializationError(f"return type {type(ret)} mismatch (expected {returntype})")
        try:
            serialized_ret = ret.serialize()
        except Exception as e:
            raise SerializationError(f"return value of type {returntype}") from e

        return serialized_ret

    return wrapper


def generate_skeleton(cls: Type[RemoteInterface]) -> Type[Skeleton]:
    if not issubclass(cls, RemoteInterface):
        raise TypeError(f"{cls} is not a remote interface")

    remote_methods = inspect.getmembers(cls, lambda v: inspect.isfunction(v) and is_remotemethod(v))
    skeleton_methods: dict[str, SkeletonMethod] = {}
    for name, method in remote_methods:
        skeleton_methods[name] = generate_skeleton_wrap(method, cls)

    return cast(Type[Skeleton], type(f"{cls.__name__}Skeleton", (Skeleton, ), skeleton_methods))


def generate_proxy():
    pass

