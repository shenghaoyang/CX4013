"""
exceptions.py

Exceptions raised by the RPC subsystem.
"""


class RPCError(Exception):
    """
    Base class for all RPC errors.
    """
    pass


class DeserializationError(RPCError):
    """
    Error raised on deserialization errors.
    """
    pass


class SerializationError(RPCError):
    """
    Error raised on serialization errors.
    """
    pass
