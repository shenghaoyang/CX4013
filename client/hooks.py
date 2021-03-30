"""
hooks.py

Client-side pre-receive and pre-send hooks used to simulate packet loss.
"""


import random
from typing import Optional
from rpc.packet import PacketFlags, PacketHeader


class RandomRequestReplyDropper:
    """
    Configurable packet dropper that drops RPC (application)
    level packets with specified probability.

    Control messages are not dropped.
    """

    CONTROL_FLAGS = (
        PacketFlags.PING,
        PacketFlags.ACK_REPLY,
        PacketFlags.RST,
        PacketFlags.CHANGE_CID,
    )

    def __init__(self, proba: float = 0.0):
        """
        Initialize a new dropper.

        :param proba: packet drop probability.
        """
        self.prob = proba

    def __call__(self, packet: bytes) -> Optional[bytes]:
        """
        Pass a packet through the dropper.

        :param packet: packet to pass.
        :return: ``None`` if the packet is to be dropped. Otherwise, the passed
            packet is returned.
        """
        # Don't alter undecodable packets.
        try:
            hdr = PacketHeader.deserialize(packet)
        except Exception:
            return packet

        # Don't drop control messages.
        if any(map(hdr.flags.__contains__, self.CONTROL_FLAGS)):
            return packet

        if not hdr.client_id.value:
            return packet

        # Calculate drop probability
        v = random.random()
        if v >= self.prob:
            return packet

        return None

    @property
    def prob(self) -> float:
        """
        Obtain the packet drop probability in the range [0, 1].
        """
        return self._proba

    @prob.setter
    def prob(self, proba: float):
        """
        Configure the packet drop probability in the range [0, 1].

        :param proba: new packet drop probability.
        """
        if not (0 <= proba <= 1):
            raise ValueError("probability must be within [0, 1].")

        self._proba = proba
