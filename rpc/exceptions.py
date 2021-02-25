"""
exceptions.py

Exceptions raised by the RPC subsystem.
"""


class RPCError(Exception):
    """
    Base class for all RPC errors.
    """

    pass


class BadRequestError(RPCError):
    """
    Client's request packet was malformed.
    """

    pass


class ArgumentDeserializationError(RPCError):
    """
    Server could not deserialize client arguments.
    """

    pass


class ArgumentSerializationError(RPCError):
    """
    Client proxy could not serialize arguments.
    """

    pass


class ReturnDeserializationError(RPCError):
    """
    Client could not deserialize server return values.
    """

    pass


class InvalidReplyError(RPCError):
    """
    Client could not decode server's reply / server's reply is not
    valid for a given context.
    """

    pass


class InternalServerFailureError(RPCError):
    """.
    Server could not execute its internal method.
    """

    pass


class MethodNotFoundError(RPCError):
    """
    Server didn't implement method requested.
    """

    pass
