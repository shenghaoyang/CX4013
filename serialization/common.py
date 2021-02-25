"""
common.py

Functionality common to all serializable types.
"""


from abc import ABC, abstractmethod


class Serializable(ABC):
    @classmethod
    @abstractmethod
    def deserialize(cls, data: bytes) -> 'Serializable':
        pass

    @property
    @abstractmethod
    def size(self) -> int:
        pass

    @abstractmethod
    def serialize(self) -> bytes:
        pass

    def copy(self) -> 'Serializable':
        return self.deserialize(self.serialize())
