"""
types.py

Data types used by the booking server.
"""


import ipaddress
from datetime import datetime, timedelta
from server.bookingtable import DateTimeRange, START_DATE
from serialization.derived import (
    create_struct_type,
    create_enum_type,
    create_array_type,
    create_union_type,
    String,
)
from typing import cast
from serialization.numeric import u8, u32


# Contains an array of Strings.
ArrayString = create_array_type("String", String)

# Encodes a day of week.
DayOfWeek = create_enum_type(
    "DayOfWeek",
    (
        "MONDAY",
        "TUESDAY",
        "WEDNESDAY",
        "THURSDAY",
        "FRIDAY",
        "SATURDAY",
        "SUNDAY",
        "NEXT_MONDAY",
    ),
)

# Array containing multiple days of the week.
ArrayDayOfWeek = create_array_type("DayOfWeek", DayOfWeek)

# Encodes a time delta.
TimeDelta = create_struct_type(
    "TimeDelta", (("hours", u8), ("minutes", u8), ("negative", u8))
)

# Encodes a timestamp.
Time = create_struct_type(
    "Time", (("hour", u8), ("minute", u8), ("dayofweek", DayOfWeek))
)

# Encodes a half-open time range, [start, end).
TimeRange = create_struct_type("TimeRange", (("start", Time), ("end", Time)))

ArrayTimeRange = create_array_type("TimeRange", TimeRange)


# Encodes a time range, or an error string.
ArrayTimeRangeOrError = create_union_type(
    "ArrayTimeRangeOrError", (("array", ArrayTimeRange), ("error", String))
)

# Encodes a booking ID, or an error string.
IDOrError = create_union_type("IDOrError", (("id", String), ("error", String)))

# Encodes an IPv4 address and a port number.
IPv4AddressPort = create_struct_type(
    "IPv4AddressPort", (("address", u32), ("port", u32))
)


def rpc_ipp_as_ipp(rpc_ipp: IPv4AddressPort) -> tuple[str, int]:
    """
    Represent a RPC ``IPv4AddressPort`` value as a tuple containing
    ``(<IPv4 address, dotted quad>, <Port number>)``.

    :param rpc_ipp: ``IPv4AddressPort`` value to use.
    """
    return ipaddress.IPv4Address(int(cast(u32, rpc_ipp["address"]))).compressed, int(
        cast(u32, rpc_ipp["port"])
    )


def ipp_as_rpc_ipp(ipp: tuple[str, int]) -> IPv4AddressPort:
    """
    Represent a tuple containing an IPv4 address and a port number
    as an RPC ``IPv4AddressPort`` object.

    :param ipp: tuple to use.
    """
    return IPv4AddressPort(
        address=u32(
            int.from_bytes(
                ipaddress.ip_address(ipp[0]).packed, byteorder="big", signed=False
            )
        ),
        port=u32(ipp[1]),
    )


def rpc_td_as_td(rpc_td: TimeDelta) -> timedelta:
    """
    Represent a RPC ``TimeDelta`` value as a ``timedelta`` instance.

    :param rpc_td: ``TimeDelta`` value to use.
    """
    mult = -1 if cast(u8, rpc_td["negative"]).value else 1

    return (
        timedelta(
            hours=cast(u8, rpc_td["hours"]).value,
            minutes=cast(u8, rpc_td["minutes"]).value,
        )
        * mult
    )


def dt_as_rpc_t(dt: datetime) -> Time:
    """
    Represent a ``datetime`` value as an RPC ``Time`` type.

    :param dt: datetime value to use.
    """
    # noinspection PyCallingNonCallable
    return Time(
        hour=u8(dt.hour),
        minute=u8(dt.minute),
        dayofweek=DayOfWeek(DayOfWeek.VALUES(dt.day - START_DATE.day)),
    )


def dtrange_as_rpc_tr(dtrange: DateTimeRange) -> TimeRange:
    """
    Represent a ``DateTimeRange`` as an RPC ``TimeRange`` type.

    :param dtrange: date time range to use.
    """
    return TimeRange(start=dt_as_rpc_t(dtrange.start), end=dt_as_rpc_t(dtrange.end))


def rpc_t_as_dt(rpc_t: Time) -> datetime:
    """
    Represent a RPC ``Time`` instance as a ``datetime``.

    :param rpc_t: time instance to use.
    :raises ValueError: if the ``Time`` instance cannot be represented as a ``datetime``.
    """
    return START_DATE + timedelta(
        hours=cast(u8, rpc_t["hour"]).value,
        minutes=cast(u8, rpc_t["minute"]).value,
        days=cast(DayOfWeek, rpc_t["dayofweek"]).value.value,
    )


def rpc_tr_as_dtrange(rpc_tr: TimeRange) -> DateTimeRange:
    """
    Represent an RPC ``TimeRange`` instance as a ``DateTimeRange``.

    :param rpc_tr: time range to use.
    :raises ValueError: if the ``TimeRange`` cannot be converted into a valid ``DateTimeRange``.
    """
    return DateTimeRange(
        start=rpc_t_as_dt(cast(Time, rpc_tr["start"])),
        end=rpc_t_as_dt(cast(Time, rpc_tr["end"])),
    )
