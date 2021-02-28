"""
packet.py

RMI / RPC-level packet definitions.
"""


from typing import Final, Type
from enum import Flag, Enum
from session.packet import SequenceNumber, U32


# Packet structure
# Byte 0:3 Transaction number, U32LE
# Byte 4 Flag byte 1
#    Bit 0: 1 -> Reply 0 -> Request
#    Bit 1: Replayed reply flag (set when server is resending cached reply)
#    Bit 2: Reply ACK flag (used to drop cached reply)
#    Bit 3: Reserved
#    Bit 4:5: Invocation semantics
#        0: At least once
#        1: At most once
#        2: Reserved
#        3: Reliable transport semantics todo: not implemented yet
#    Bit 6-7: Reserved.
# Byte 5-8: Method ordinal
# Byte 9 - Return value / arguments.


class PacketFlags(Flag):
    NONE = 0
    REPLY = 1 << 0
    RE_REPLY = 1 << 1
    ACK_REPLY = 1 << 2


class InvocationSemantics(Enum):
    AT_LEAST_ONCE = 0
    AT_MOST_ONCE = 1
    RELIABLE_TRANSPORT = 3


class PacketHeader:
    LENGTH: SequenceNumber.LENGTH + 1 + U32.LENGTH

    def __init__(self, trans_num: SequenceNumber = SequenceNumber(0), flags: PacketFlags = PacketFlags.NONE,
                 semantics: InvocationSemantics = InvocationSemantics.AT_LEAST_ONCE,
                 method_ordinal: U32 = U32(0), payload: bytes = b''):
        self._trans_num = trans_num
        self._flags = flags
        self._semantics = semantics
        self._method_ordinal = method_ordinal
        self._payload = payload

    @classmethod
    def from_bytes(cls: Type['PacketHeader'], data: bytes) -> 'PacketHeader':
        if len(data) < cls.LENGTH:
            raise ValueError(f"data too short")

        seq = SequenceNumber.from_bytes()
        off = SequenceNumber.LENGTH



    @property
    def trans_num(self) -> SequenceNumber:
        return self._trans_num

    @trans_num.setter
    def trans_num(self, new: SequenceNumber):
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
    def method_ordinal(self) -> U32:
        return self._method_ordinal

    @method_ordinal.setter
    def method_ordinal(self, new: U32):
        self._method_ordinal = new

    @property
    def payload(self) -> bytes:
        return self._payload

    @payload.setter
    def payload(self, new: bytes):
        self._payload = new


