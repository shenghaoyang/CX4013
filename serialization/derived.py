"""
derived.py

Derived types for the CAL RPC protocol.
"""


from serialization.common import Serializable
from serialization.numeric import u64
from collections.abc import MutableSequence, Sequence
from typing import Tuple, Type, TypeVar, Union, Iterable, Optional, cast, Mapping
from enum import Enum

T = TypeVar("T")


class Struct(Serializable):
    """
    Base class representing a CAL RPC structure.
    """

    ELEMENTS: Tuple[Tuple[str, Type[Serializable]], ...] = tuple()

    def __init__(self, **kwargs):
        self._values_keyed = dict()
        for key, type_ in self.ELEMENTS:
            # zero-initialize all values
            self._values_keyed[key] = type_()

        for k, v in kwargs.items():
            if k not in self._values_keyed:
                raise KeyError(f"element {k} does not exist")

            self._values_keyed[k] = v.copy()

    def __repr__(self) -> str:
        params = ", ".join(f"{t[0]}={self[t[0]]!r}" for t in self.ELEMENTS)
        return f"{self.__class__.__name__}({params})"

    @classmethod
    def deserialize(cls, data: bytes) -> "Struct":
        off = 0
        out = cls()
        for key, type_ in cls.ELEMENTS:
            out[key] = type_.deserialize(data[off:])
            off += out[key].size

        return out

    def __getitem__(self, key: str) -> Serializable:
        return self._values_keyed[key]

    def __setitem__(self, key: str, value: Serializable):
        if key not in self._values_keyed:
            raise KeyError(key)
        cur = self[key]
        if not isinstance(value, type(cur)):
            raise TypeError(f"{value!r} of wrong type for element {key}")

        self._values_keyed[key] = value.copy()

    @property
    def size(self) -> int:
        return sum(v.size for v in self._values_keyed.values())

    def serialize(self) -> bytes:
        out = bytearray()
        for key, _ in self.ELEMENTS:
            out.extend(self[key].serialize())

        return out


def create_struct_type(
    name: str, elements: Sequence[Tuple[str, Type[Serializable]], ...] = tuple()
) -> Type[Struct]:
    """
    Create a new type representing a CAL RPC structure.
    """
    # Some type checkers cannot infer that result is a type that has Struct as a base class.
    return cast(Type[Struct], type(name, (Struct,), {"ELEMENTS": tuple(elements)}))


class Array(Serializable, MutableSequence[Serializable]):
    """
    Abstract base class representing a CAL RPC array.
    """

    TYPE: Type[Serializable] = Serializable
    """  
    Base class representing a CAL RPC Variable-length array.
    """

    def __init__(self, values: Iterable[Serializable] = tuple()):
        self._values = list(values)

    def __repr__(self) -> str:
        params = ", ".join(repr(v) for v in self)
        return f"{self.__class__.__name__}([{params}])"

    def __len__(self) -> int:
        return len(self._values)

    def __getitem__(
        self, i: Union[int, slice]
    ) -> Union[MutableSequence[Serializable], Serializable]:
        return self._values[i]

    def __setitem__(
        self, i: Union[int, slice], o: Union[Serializable, Iterable[Serializable]]
    ):
        sto = (
            self._type_check(o)
            if isinstance(i, int)
            else (self._type_check(v).copy() for v in o)
        )

        self._values[i] = sto

    def __delitem__(self, i: Union[int, slice]):
        del self._values[i]

    def _type_check(self, o: T) -> T:
        if not isinstance(o, self.TYPE):
            raise TypeError(f"{o!r} is of wrong type for array")

        return o

    @classmethod
    def deserialize(cls, data: bytes) -> "Array":
        sz = u64.deserialize(data)
        off = sz.size
        values = list()
        for i in range(sz.value):
            values.append(cls.TYPE.deserialize(data[off:]))
            off += values[-1].size

        return cls(values)

    def insert(self, i: int, v: Serializable):
        self._values.insert(i, v)

    @property
    def size(self) -> int:
        return u64().size + sum(elem.size for elem in self._values)

    def serialize(self) -> bytes:
        out = bytearray()
        sz = u64(len(self._values))

        out.extend(sz.serialize())
        for v in self._values:
            out.extend(v.serialize())

        return out


def create_array_type(
    elem_type_name: str, elem_type: Type[Serializable]
) -> Type[Array]:
    """
    Create a new type representing a CAL RPC array.

    :param elem_type_name: The CAL RPC type name of the elements in the array.
    :param elem_type: Python type representing the CAL RPC type of the elements in the array.
    """
    name = f"{elem_type_name}Array"
    # Some type checkers cannot infer that result is a type that has Array as a base class.
    return cast(Type[Array], type(name, (Array,), {"TYPE": elem_type}))


class Union_(Serializable):
    """
    Abstract base class representing a CAL RPC Union.
    """

    TAG_TO_NAME: Mapping[int, str] = dict()
    NAME_TO_TAG: Mapping[str, int] = dict()
    NAME_TO_TYPE: Mapping[str, Type[Serializable]] = dict()

    def __init__(self, name: str, value: Serializable):
        self[name] = value

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name}, {self.value!r})"

    def __contains__(self, item: str) -> bool:
        return self.name == item

    def __getitem__(self, name: str) -> Serializable:
        if self._name != name:
            raise KeyError(f"{name} not in union")

        return self._value

    def __setitem__(self, name: str, value: Serializable):
        # Needed __getitem__ to avoid type checkers thinking that self._allowed_elements[name] is a
        # parameterized generic type.
        if not isinstance(value, self.NAME_TO_TYPE.__getitem__(name)):
            raise TypeError(f"type mismatch for element {name}")

        self._value = value.copy()
        self._name = name

    @classmethod
    def deserialize(cls, data: bytes) -> "Union_":
        tag = u64.deserialize(data)
        off = tag.size
        name = cls.TAG_TO_NAME[tag.value]
        type_ = cls.NAME_TO_TYPE[name]

        return cls(name, type_.deserialize(data[off:]))

    @property
    def size(self) -> int:
        return u64().size + self.value.size

    @property
    def value(self) -> Serializable:
        return self._value

    @property
    def name(self) -> str:
        return self._name

    def serialize(self) -> bytes:
        out = bytearray()
        tag = u64(self.NAME_TO_TAG[self.name])
        out.extend(tag.serialize())
        out.extend(self.value.serialize())

        return out


def create_union_type(
    name: str, elements: Sequence[Tuple[str, Type[Serializable]], ...] = tuple()
) -> Type[Union_]:
    """
    Create a new type representing a CAL RPC Union.

    :param name: The CAL RPC type name of the union.
    :param elements: elements contained within the union, as a sequence of tuples of
        ``(element name, element type)`` pairs.
    """
    # Some type checkers cannot infer that result is a type that has Array as a base class.
    tag_to_name = dict()
    name_to_tag = dict()
    name_to_type = dict()

    for i, (name, type_) in enumerate(elements):
        tag_to_name[i] = name
        name_to_tag[name] = i
        name_to_type[name] = type_

    return cast(
        Type[Union_],
        type(
            name,
            (Union_,),
            {
                "TAG_TO_NAME": tag_to_name,
                "NAME_TO_TAG": name_to_tag,
                "NAME_TO_TYPE": name_to_type,
            },
        ),
    )


class Enum_(Serializable):
    """
    Abstract base class representing a CAL RPC enumeration.

    To represent a new named Enumeration type, subclass this class
    and override the ``VALUES`` attribute with an Enumeration class
    generated from ``create_enum()``.
    """

    VALUES: Enum = Enum("Dummy", "DO NOT USE")
    INDEX_TO_VALUES: Sequence[VALUES] = tuple()

    def __init__(self, value: Optional[VALUES] = None):
        # Some type checkers don't know that Enum type objects are iterable
        self.value = (
            next(iter(cast(Iterable[self.VALUES], self.VALUES)))
            if value is None
            else value
        )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.value!r})"

    @classmethod
    def deserialize(cls, data: bytes) -> "Enum_":
        val = u64.deserialize(data)
        return cls(cls.INDEX_TO_VALUES[val.value])

    @property
    def size(self) -> int:
        return u64().size

    @property
    def value(self) -> VALUES:
        return self._value

    @value.setter
    def value(self, value: VALUES):
        if value not in self.VALUES:
            raise TypeError(f"{value!r} is not a value of this enum")

        self._value = value

    def serialize(self) -> bytes:
        return u64(self.value.value).serialize()


def create_enum_type(name: str, values: Sequence[str]) -> Type[Enum_]:
    """
    Create a new type representing a CAL RPC enumerated type.

    :param name: name of the enumeration type created.
    :param values: values of the enumeration type, in the same order as they were
        declared in the RPC specification.
    """
    enum = Enum(f"{name}_VALUES", values, start=0)
    index_to_values = tuple(enum)
    return cast(
        Type[Enum_],
        type(name, (Enum_,), {"VALUES": enum, "INDEX_TO_VALUES": index_to_values}),
    )


class String(Serializable):
    """
    Class representing a CAL RPC String.
    """

    def __init__(self, val: str = ""):
        self.value = val

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.value!r})"

    @classmethod
    def deserialize(cls, data: bytes) -> "String":
        length = u64.deserialize(data)
        off = length.size
        return cls(data[off : off + length.value].decode("UTF-8"))

    @property
    def size(self) -> int:
        return u64().size + len(self.value.encode("UTF-8"))

    @property
    def value(self) -> str:
        return self._value

    @value.setter
    def value(self, val: str):
        if not isinstance(val, str):
            raise TypeError(f"{val!r} is not a Python string")
        self._value = val

    def serialize(self) -> bytes:
        out = bytearray()
        out.extend(u64(len(self.value)).serialize())
        out.extend(self.value.encode("UTF-8"))
        return out
