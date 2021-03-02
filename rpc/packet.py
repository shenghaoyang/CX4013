"""
packet.py

RMI / RPC-level packet definitions.
"""


import random
from rpc import exceptions
from typing import Final, Type, Optional
from enum import Flag, Enum, auto
from serialization.numeric import u32

# Packet structure
# Byte 0:3 Client identifier, u32
# Byte 4:7 Transaction number, u32
# Byte 8: Flags
#    Bit 0: 1 -> Reply 0 -> Request
#    Bit 1: Replayed reply flag (set when server is resending cached reply)
#    Bit 2: Reply ACK flag (used to drop cached reply)
#    Bit 3: Change CID flag (set by server when replying with a new CID, bootstraping for CID == 0 clients)
#    Bit 4: PING flag
#    Bit 5: RST flag
#    Bit 6:7: Invocation semantics
#        0: At least once
#        1: At most once
#        2: Reserved
#        3: Reliable transport semantics
# Byte 9-12: Method ordinal, u32
# If request:
#   Byte 13:End: Serialized arguments
# If response:
#   Byte 13: Execution status
#   Byte 14:End: Serialized response


class TransactionID(u32):
    """
    Transaction ID that is sent to the server to match with transaction IDs
    received from the client.
    """

    @classmethod
    def random(cls: Type['TransactionID']) -> "TransactionID":
        """
        Create a random Transaction ID.
        """
        return cls(random.randint(u32.min(), u32.max()))

    def next(self) -> u32:
        """
        Increment the transaction ID, modulo its maximum value.

        Returns the transaction ID.
        """
        self.value = (self.value + 1) % self.max()
        return self


class PacketFlags(Flag):
    # No flag set.
    NONE = 0
    # Packet is a reply.
    REPLY = 1 << 0
    # Packet is a resent reply / resent request.
    REPLAYED = 1 << 1
    # Packet is an ACK to a previous reply.
    # Only used for invocations with at-most-once semantics.
    ACK_REPLY = 1 << 2
    # Packet contains new CID for the client.
    # Client should only change if it was previously using a CID of 0.
    CHANGE_CID = 1 << 3
    # Packet is a PING reply / request.
    PING = 1 << 4
    # Packet is a RESET.
    RST = 1 << 5


class ExecutionStatus(Enum):
    """
    Status of executing the remote method.
    """

    # Execution succeeded
    OK = auto()
    # Bad client request
    BAD_REQUEST = auto()
    # Method with specified ordinal doesn't exist
    NO_SUCH_METHOD = auto()
    # Failed to deserialize client arguments
    DESERIALIZATION_FAILED = auto()
    # Remote method failed (RPC machinery worked, but impl. called by skeleton bombed out).
    INTERNAL_FAILURE = auto()


def estatus_to_exception(s: ExecutionStatus) -> Optional[Type[exceptions.RPCError]]:
    """
    Convert the execution status code to an RPC exception.

    :param s: execution status to convert.
    :return: ``None`` if it does not represent an exception, else the type of exception
        corresponding to the execution status.
    """
    return {
        ExecutionStatus.OK: None,
        ExecutionStatus.BAD_REQUEST: exceptions.BadRequestError,
        ExecutionStatus.NO_SUCH_METHOD: exceptions.MethodNotFoundError,
        ExecutionStatus.DESERIALIZATION_FAILED: exceptions.ArgumentDeserializationError,
        ExecutionStatus.INTERNAL_FAILURE: exceptions.InternalServerFailureError,
    }[s]


def exception_to_estatus(e: Type[exceptions.RPCError]) -> Optional[ExecutionStatus]:
    """
    Convert the RPC exception to an execution status code.

    :param e: RPC exception.
    :return: ``None`` if inconvertible, else the converted status code.
    """
    mapping: dict[Type[exceptions.RPCError], ExecutionStatus] = {
        exceptions.BadRequestError: ExecutionStatus.BAD_REQUEST,
        exceptions.MethodNotFoundError: ExecutionStatus.NO_SUCH_METHOD,
        exceptions.ArgumentDeserializationError: ExecutionStatus.DESERIALIZATION_FAILED,
        exceptions.InternalServerFailureError: ExecutionStatus.INTERNAL_FAILURE,
    }

    return mapping.get(e)


class InvocationSemantics(Enum):
    """
    Invocation semantics for remote method execution.
    """

    AT_LEAST_ONCE = 0
    AT_MOST_ONCE = 1
    # Reliable transport semantics.
    # Not implemented.
    # RELIABLE_TRANSPORT = 3


class PacketHeader:
    # Length of the packet header
    LENGTH: Final = 2 * u32().size + 1 + u32().size

    def __init__(
        self,
        client_id: u32 = None,
        trans_num: u32 = None,
        flags: PacketFlags = PacketFlags.NONE,
        semantics: InvocationSemantics = InvocationSemantics.AT_LEAST_ONCE,
        method_ordinal: u32 = None,
    ):
        """
        Create a new packet header.

        :param client_id: client identifier.
        :param trans_num: transaction number.
        :param flags: flags associated with the packet.
        :param semantics: invocation semantics.
        :param method_ordinal: ordinal of method to invoke on the remote side.
        """
        self._client_id = client_id if client_id is not None else u32()
        self._trans_num = trans_num if trans_num is not None else u32()
        self._flags = flags
        self._semantics = semantics
        self._method_ordinal = method_ordinal if method_ordinal is not None else u32()

    @classmethod
    def deserialize(cls: Type["PacketHeader"], data: bytes) -> "PacketHeader":
        """
        Recover a packet header from raw bytes.

        :param data: data representing the packet header.
        """
        if len(data) < cls.LENGTH:
            raise ValueError(f"data too short")

        cid = u32.deserialize(data)
        off = cid.size

        tid = u32.deserialize(data[off:])
        off += tid.size

        flags = PacketFlags(data[off] & 0x3F)
        semantics = InvocationSemantics(data[off] >> 6)

        off += 1
        ordinal = u32.deserialize(data[off:])

        return cls(cid, tid, flags, semantics, ordinal)

    @property
    def size(self) -> int:
        return self.LENGTH

    @property
    def client_id(self) -> u32:
        return self._client_id

    @client_id.setter
    def client_id(self, new: u32):
        self._client_id = new

    @property
    def trans_num(self) -> u32:
        return self._trans_num

    @trans_num.setter
    def trans_num(self, new: u32):
        self._trans_num = new

    @property
    def flags(self) -> PacketFlags:
        return self._flags

    @flags.setter
    def flags(self, new: PacketFlags):
        self._flags = new

    @property
    def semantics(self) -> InvocationSemantics:
        return self._semantics

    @semantics.setter
    def semantics(self, new: InvocationSemantics):
        self._semantics = new

    @property
    def method_ordinal(self) -> u32:
        return self._method_ordinal

    @method_ordinal.setter
    def method_ordinal(self, new: u32):
        self._method_ordinal = new

    @property
    def is_reply(self) -> bool:
        return bool(self.flags & self.flags.REPLY)

    def serialize(self) -> bytes:
        """
        Convert this header to a sequence of bytes.
        """
        out = bytearray()
        out.extend(self.client_id.serialize())
        out.extend(self.trans_num.serialize())
        out.append(self.flags.value | (self.semantics.value << 6))
        out.extend(self.method_ordinal.serialize())

        return out
