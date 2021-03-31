"""
common.py

Functionality common to all serializable types.
"""


from abc import ABC, abstractmethod


class Serializable(ABC):
    """
    Base class for all serializable types.
    """

    @classmethod
    @abstractmethod
    def deserialize(cls, data: bytes) -> "Serializable":
        """
        Deserialize provided data.

        :param data: data to deserialize.
        :return: a ``Serializable`` instance.
        """
        pass

    @property
    @abstractmethod
    def size(self) -> int:
        """
        Obtain the size of this object when serialized, in bytes.
        """
        pass

    @abstractmethod
    def serialize(self) -> bytes:
        """
        Serialize this object.
        """
        pass

    def copy(self) -> "Serializable":
        """
        Copy this object.

        Copies are deep. The return value can be safely mutated without affecting
        this object.
        """
        return self.deserialize(self.serialize())
