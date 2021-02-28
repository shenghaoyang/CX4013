"""
packet.py

Types used in the CAL RPC session protocol.
"""

import random
import struct
from enum import Enum, Flag
from typing import Final, Type, Union, Optional
from functools import total_ordering

HEADER_MAGIC: Final = b'CL'


# Packet definition
# 0 - 1: HEADER_MAGIC
# 2 - 5: Sequence u32 LE
# 6 - 9: Channel u32 LE, 0 reserved for session-wide.
# 10: Flags u16 LE
#    0: SYN (Open session / open channel)
#    1: ACK (Acknowledge ACKable)
#    6: FIN (Close session / close channel)
#    7: RST (Reset session / channel)
# 11 - end: Payload
#     Flags = 0 -> Upper layer PDU
#     OPEN CHANNEL -> NUL-terminated service name. UTF-8.
#     ACK -> Sequence number of packet to ACK, u32 LE


class PacketFlags(Flag):
    """
    Packet flags.
    """
    NONE = 0
    SYN = 1 << 0
    ACK = 1 << 1
    FIN = 1 << 6
    RST = 1 << 7


class SessionPacketType(Enum):
    """
    Packet types for session-wide packets.
    """
    # Open session -> respond and allocate endpoint structure.
    OPEN_SESSION = PacketFlags.SYN

    # Close session -> respond and deallocate endpoint structure.
    CLOSE_SESSION = PacketFlags.FIN

    # Reset session -> non-graceful disconnect.
    RESET_SESSION = PacketFlags.RST


class ChannelPacketType(Enum):
    """
    Packet types for channel-specific packets.
    """
    # Open channel -> respond and allocate channel structure.
    OPEN_CHANNEL = PacketFlags.SYN

    # Close channel -> respond and destroy channel structures.
    CLOSE_CHANNEL = PacketFlags.FIN

    # Reset channel -> non-graceful disconnect.
    RESET_CHANNEL = PacketFlags.RST

    # Upper layer protocol data.
    UL_PDU = PacketFlags.NONE


@total_ordering
class U32:
    """
    Class representing a 4-byte (unsigned) integer number.

    This is distinct from the CAL RPC type of similar name.
    """
    MAX: Final = (2 ** 32) - 1
    MIN: Final = 0
    PACK_FORMAT: Final = "<I"
    LENGTH: Final = struct.calcsize(PACK_FORMAT)

    def __init__(self, initial: int):
        self.value = initial

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.value})"

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented

        return self.value == other.value

    def __lt__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented

        return self.value < other.value

    @classmethod
    def from_bytes(cls: Type['U32'], data: bytes) -> 'U32':
        if len(data) != 4:
            raise ValueError(f"data not 32-bits")

        return cls(struct.unpack(cls.PACK_FORMAT, data)[0])

    @property
    def value(self) -> int:
        """
        Return the value of this number as an integer.
        """
        return self._value

    @value.setter
    def value(self, new: int):
        """
        Set the value of this number.
        """
        if not (self.MIN <= new <= self.MAX):
            raise ValueError(f"sequence number {new} out of bounds")

        self._value = new

    def to_bytes(self) -> bytes:
        """
        Represent this number as a 4-byte unsigned integer.
        """
        return struct.pack(self.PACK_FORMAT, self.value)


class SequenceNumber(U32):
    """
    Class representing a 4-byte (unsigned) sequence number.
    """

    def next(self) -> 'SequenceNumber':
        """
        Increment the sequence number and return the sequence number object.
        """
        self.value = (self.value + 1) % self.MAX
        return self

    def distance(self, other: 'SequenceNumber') -> int:
        """
        Return the clockwise distance from the current sequence number to the passed one.
        """
        return (other.value - self.value) % self.MAX

    def randomize(self) -> 'SequenceNumber':
        """
        Randomize the sequence number by choosing a number within ``[MIN, MAX]``
        with uniform probability.

        The sequence number object is returned.
        """
        self.value = random.randint(self.MIN, self.MAX)
        return self


class PacketHeader:
    """
    Data type used to represent a CAL RPC session protocol layer packet header.
    """
    LENGTH: Final = len(HEADER_MAGIC) + SequenceNumber.LENGTH + U32.LENGTH + 1

    def __init__(self, flags: PacketFlags, channel: U32 = U32(0), seq: Optional[SequenceNumber] = None):
        """
        Create a new packet header.

        :param seq: packet sequence number.
        :param channel: packet channel.
        :param flags: packet flags.
        """
        self._seq = seq
        self._channel = channel
        self._flags = flags

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.flags}, {self.channel}, {self.sequence if self._seq is not None else None})"

    @classmethod
    def from_bytes(cls: Type['PacketHeader'], data: bytes) -> 'PacketHeader':
        if len(data) < cls.LENGTH:
            raise ValueError(f"data only {len(data)} bytes long")

        if data[:len(HEADER_MAGIC)] != HEADER_MAGIC:
            raise ValueError(f"header magic mismatch")

        off = len(HEADER_MAGIC)
        seq = SequenceNumber.from_bytes(data[off:off + SequenceNumber.LENGTH])
        off += SequenceNumber.LENGTH

        channel = U32.from_bytes(data[off: off + U32.LENGTH])
        off += U32.LENGTH

        flags = PacketFlags(data[off])

        return cls(flags, channel, seq)

    @property
    def sequence(self) -> SequenceNumber:
        if self._seq is None:
            raise RuntimeError("sequence number not set")
        return self._seq

    @sequence.setter
    def sequence(self, new: SequenceNumber):
        self._seq = new

    @property
    def channel(self) -> U32:
        return self._channel

    @channel.setter
    def channel(self, new: U32):
        self._channel = new

    @property
    def flags(self) -> PacketFlags:
        return self._flags

    @flags.setter
    def flags(self, new: PacketFlags):
        self._flags = new

    @property
    def ack(self) -> bool:
        """
        Checks whether this packet header is from an acknowledgement packet.
        """
        return PacketFlags.ACK in self.flags

    @property
    def session(self) -> bool:
        """
        Checks whether this packet header is from a session-wide packet.
        """
        return self.channel.value == 0

    def classify(self) -> Union[SessionPacketType, ChannelPacketType]:
        """
        Classifies the packet from the header.
        """
        try:
            flags = self.flags
            if self.ack:
                flags ^= PacketFlags.ACK

            if self.session:
                return SessionPacketType(flags)

            return ChannelPacketType(flags)
        except ValueError:
            raise RuntimeError("unknown packet type")

    def to_bytes(self) -> bytes:
        out = bytearray()
        out.extend(HEADER_MAGIC)
        out.extend(self.sequence.to_bytes())
        out.extend(self.channel.to_bytes())
        out.append(self.flags.value)

        return out
