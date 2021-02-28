"""
numeric.py

Numeric type support for the CAL RPC (Python) protocol compiler.
"""


from serialization.common import Serializable
from abc import abstractmethod


class Numeric(Serializable):
    """
    Abstract base class representing a numeric CAL RPC type.
    """
    def __init__(self, value: int = 0):
        self._v = 0
        self.value = value

    @classmethod
    def deserialize(cls, data: bytes) -> 'Numeric':
        if len(data) < cls().size:
            raise ValueError(f"data too short")
        return cls(int.from_bytes(data[:cls().size], 'little', signed=cls.signed()))

    @classmethod
    def bit_size(cls) -> int:
        ist = cls()
        return ist.size * 8

    @classmethod
    def max(cls) -> int:
        if cls.signed():
            return 2 ** (cls.bit_size() - 1) - 1
        else:
            return 2 ** cls.bit_size() - 1

    @classmethod
    def min(cls) -> int:
        if cls.signed():
            return -(2 ** (cls.bit_size() - 1))
        else:
            return 0

    @staticmethod
    @abstractmethod
    def signed() -> bool:
        pass

    def serialize(self) -> bytes:
        return self._v.to_bytes(self.size, 'little', signed=self.signed())

    @property
    def value(self) -> int:
        return self._v

    @value.setter
    def value(self, val: int):
        saved = self.value
        try:
            self._v = val
            self.serialize()
        except OverflowError:
            self._v = saved
            raise


class i8(Numeric):
    @property
    def size(self) -> int:
        return 1

    @staticmethod
    def signed() -> bool:
        return True


class u8(Numeric):
    @property
    def size(self) -> int:
        return 1

    @staticmethod
    def signed() -> bool:
        return False


class i32(Numeric):
    @property
    def size(self) -> int:
        return 4

    @staticmethod
    def signed() -> bool:
        return True


class u32(Numeric):
    @property
    def size(self) -> int:
        return 4

    @staticmethod
    def signed() -> bool:
        return False


class i64(Numeric):
    @property
    def size(self) -> int:
        return 8

    @staticmethod
    def signed() -> bool:
        return True


class u64(Numeric):
    @property
    def size(self) -> int:
        return 8

    @staticmethod
    def signed() -> bool:
        return False
