"""
test_derived.py

Tests for derived type serialization.
"""


import pytest
from serialization.derived import (
    create_struct_type,
    create_array_type,
    create_union_type,
    create_enum_type,
    String,
)
from serialization.numeric import i64, u64


Point = create_struct_type("Point", (("x", i64), ("y", i64)))
PointArray = create_array_type("Point", Point)
PointOrPointArray = create_union_type(
    "PointOrPointArray", (("point", Point), ("point_array", PointArray))
)
Color = create_enum_type("Color", ("RED", "GREEN", "BLUE"))


@pytest.fixture
def simple_point() -> Point:
    p = Point()
    p["x"] = i64(0xDEAD)
    p["y"] = i64(0xBEEF)

    return p


def point_equals(p1: Point, p2: Point) -> bool:
    return (p1["x"].value == p2["x"].value) and (p1["y"].value == p2["y"].value)


def point_array_equals(a1: PointArray, a2: PointArray) -> bool:
    return (len(a1) == len(a2)) and all(point_equals(v1, v2) for v1, v2 in zip(a1, a2))


class TestStruct:
    def test_round_trip(self, simple_point: Point):
        recovered = Point.deserialize(simple_point.serialize())
        assert point_equals(recovered, simple_point)

    def test_cannot_assign_wrong_type(self, simple_point: Point):
        with pytest.raises(TypeError):
            simple_point["x"] = u64()

    def test_size_property(self, simple_point: Point):
        assert simple_point.size == (u64().size + u64().size)

    def test_correct_serialized_size(self, simple_point: Point):
        assert len(simple_point.serialize()) == simple_point.size


@pytest.fixture
def simple_array() -> PointArray:
    p1 = Point()
    p2 = Point()

    p1["x"] = i64(0)
    p1["y"] = i64(10)
    p2["x"] = i64(1)
    p2["y"] = i64(20)

    arr = PointArray()
    arr.extend((p1, p2))

    return arr


class TestArray:
    def test_round_trip(self, simple_array: PointArray):
        recovered = PointArray.deserialize(simple_array.serialize())
        assert point_array_equals(recovered, simple_array)

    def test_cannot_assign_wrong_type(self, simple_array: PointArray):
        with pytest.raises(TypeError):
            simple_array[0] = u64(0)

    def test_size_property(self, simple_array: PointArray):
        assert simple_array.size == (2 * Point().size + u64().size)

    def test_correct_serialized_size(self, simple_array: PointArray):
        assert len(simple_array.serialize()) == simple_array.size


@pytest.fixture
def simple_union() -> PointOrPointArray:
    out = PointOrPointArray("point_array", PointArray())

    return out


class TestUnion:
    def test_round_trip(self, simple_union: PointOrPointArray):
        recovered = simple_union.deserialize(simple_union.serialize())
        assert recovered.name == simple_union.name
        assert point_array_equals(recovered.value, simple_union.value)

    def test_cannot_assign_wrong_type(self, simple_union: PointOrPointArray):
        with pytest.raises(TypeError):
            simple_union["point"] = u64()

    def test_value_and_name_property(self, simple_union: PointOrPointArray):
        assert simple_union.name == "point_array"
        assert "point_array" in simple_union
        assert "point" not in simple_union
        assert type(simple_union.value) is PointArray

        point = Point()
        simple_union["point"] = point

        assert simple_union.name == "point"
        assert type(simple_union.value) is Point

    def test_size_property(self, simple_union: PointOrPointArray):
        assert simple_union.size == (PointArray().size + u64().size)

        point = Point()
        simple_union["point"] = point

        assert simple_union.size == (point.size + u64().size)

    def test_correct_serialized_size(self, simple_union: PointOrPointArray):
        assert len(simple_union.serialize()) == simple_union.size


@pytest.fixture
def simple_color() -> Color:
    return Color(Color.VALUES.GREEN)


class TestEnum:
    def test_round_trip(self, simple_color: Color):
        recovered = Color.deserialize(simple_color.serialize())
        assert recovered.value == simple_color.value

    def test_cannot_assign_wrong_type(self, simple_color: Color):
        with pytest.raises(TypeError):
            simple_color.value = 0

    def test_size_property(self, simple_color: Color):
        assert simple_color.size == u64().size

    def test_correct_serialized_size(self, simple_color: Color):
        assert len(simple_color.serialize()) == simple_color.size


@pytest.fixture
def simple_string() -> String:
    return String("CE4013")


class TestString:
    def test_round_trip(self, simple_string: String):
        recovered = String.deserialize(simple_string.serialize())
        assert recovered.value == simple_string.value

    def test_cannot_assign_wrong_type(self, simple_string: String):
        with pytest.raises(TypeError):
            simple_string.value = 0

    def test_size_property(self, simple_string: String):
        assert simple_string.size == (
            u64().size + len(simple_string.value.encode("UTF-8"))
        )

    def test_correct_serialized_size(self, simple_string: String):
        assert len(simple_string.serialize()) == simple_string.size
