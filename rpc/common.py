"""
common.py

Common RPC definitions.
"""


import inspect
from typing import Callable, Any
from typing_extensions import Protocol
from serialization.common import Serializable
from abc import ABC
from collections import Awaitable


class RemoteMethod(Protocol):
    def __call__(self, obj: Any, *args: Serializable) -> Awaitable[Serializable]:
        pass


def remotemethod(func: Callable) -> Callable:
    """
    Decorator used to tag a callable as a remote method.

    :param func: function to tag.
    :return: tagged function.
    """
    signature = inspect.signature(func)
    for i, (name, parameter) in enumerate(signature.parameters.items()):
        if not i:
            # skip "self"
            continue

        if name.startswith("_"):
            # todo: maybe check during the decorator?
            raise ValueError(f"remote method cannot start with _")

        if parameter.kind != inspect.Parameter.POSITIONAL_OR_KEYWORD:
            raise TypeError(
                f"parameter {name} is not of " f"positional or keyword type"
            )

        if not issubclass(parameter.annotation, Serializable):
            raise TypeError(f"parameter {name} is not serializable")

    if hasattr(func, "_calrpc_remote"):
        raise ValueError(f"callable {func} has pre-existing tag")

    func._calrpc_remote = True
    return func


def is_remotemethod(func: Callable) -> bool:
    return hasattr(func, "_calrpc_remote")


class RemoteInterface(ABC):
    """
    Class used to tag a class as a remote interface.

    Skeletons and proxies are generated using this remote interface.

    Define remote interfaces and implement them with concrete classes.
    """

    pass
