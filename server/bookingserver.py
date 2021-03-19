"""
bookingserver.py

Implementation of a remote class that can be used to interact with the
booking system.
"""


from binascii import a2b_base64, b2a_base64
from datetime import timedelta, datetime
from typing import Mapping, cast
from server.bookingtable import Table, DateTimeRange, START_DATE
from serialization.derived import (
    create_struct_type,
    create_enum_type,
    create_array_type,
    create_union_type,
    String,
)
from serialization.numeric import u8
from rpc.common import RemoteInterface, remotemethod
from rpc.proxy import generate_proxy


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


class BookingServer(RemoteInterface):
    @remotemethod
    async def query_availability(
        self, facility: String, days: ArrayDayOfWeek
    ) -> ArrayTimeRangeOrError:
        """
        Query for the availability of a given facility.

        :param facility: facility name.
        :param days: days to query availability for.
        :return: array of time ranges that the facility is available for, or an error string.
            time ranges will be sorted in order of ascending time values.
        """

    @remotemethod
    async def book(self, facility: String, trange: TimeRange) -> IDOrError:
        """
        Book a facility.

        :param facility: facility name.
        :param trange: time range to book for.
        :return: booking ID or an error string.
        """

    @remotemethod
    async def modify(self, bid: String, delta: TimeDelta) -> IDOrError:
        """
        Modify a booking.

        :param bid: booking ID.
        :param delta: amount of time to shift booking for.
        :return: original booking ID or an error string.
        """

    @remotemethod
    async def cancel(self, bid: String) -> IDOrError:
        """
        Cancels an existing booking.

        Not idempotent.

        :param bid: booking ID.
        :return: original booking ID or an error string.
        """

    @remotemethod
    async def facilities(self) -> ArrayString:
        """
        Obtains the facilities available for booking.

        :return: array of strings representing the names of facilities available.
        """


class BookingServerImpl(BookingServer):
    def __init__(self, bt: Table):
        """
        Create a new booking server.

        :param bt: booking table to refer to.
        """
        self._bt = bt
        # todo lock database against concurrent modification.
        # might not be required since we don't come across any async
        # preemption points during modification of the database

    def _split_and_validate_bid(self, bid: str) -> tuple[str, int]:
        """
        Split a booking ID into its constituent facility name and facility-specific booking id.

        Also checks if the booking ID is valid by looking up the facility ID and facility-specific
        booking ID.

        :param bid: booking ID to split.
        :return: tuple ``(fid, fbid)``.
        :raises ValueError: on invalid booking id / id that corresponds to nonexistent booking.
        :raises KeyError: if booking does not exist.
        """
        split = bid.split("-")
        if len(split) != 2:
            raise ValueError(f"expected single '-' in id")

        try:
            facility = a2b_base64(split[0]).decode("utf-8")
            bid = int(split[1])
        except ValueError:
            raise ValueError("ID malformed")

        try:
            self._bt.lookup(facility, bid)
        except KeyError:
            raise

        return facility, bid

    @remotemethod
    async def query_availability(
        self, facility: String, days: ArrayDayOfWeek
    ) -> ArrayTimeRangeOrError:
        """
        Query for the availability of a given facility.

        :param facility: facility name.
        :param days: days to query availability for.
        :return: array of time ranges that the facility is available for, or an error string.
            time ranges will be sorted in order of ascending time values.
        """
        facility = facility.value

        if not len(days):
            return ArrayTimeRangeOrError("error", String("no days requested"))

        days: list[DayOfWeek.VALUES] = list(set(cast(DayOfWeek, d).value for d in days))
        days.sort(key=lambda v: v.value)

        # Already sorted in ascending order.
        try:
            bookings: list[DateTimeRange] = list(
                map(lambda t: t[1].as_dtrange(), self._bt.list_bookings(facility))
            )
        except KeyError as e:
            return ArrayTimeRangeOrError("error", String(e.args[0]))

        out = ArrayTimeRange()
        for d in days:
            start = START_DATE + timedelta(days=d.value)
            end = start + timedelta(days=1)

            def bookings_involving_today():
                today_start = start
                today_end = end
                for b in bookings:
                    start_ok = b.start < today_end
                    end_ok = today_start < b.end
                    if not (start_ok and end_ok):
                        continue

                    yield b

            # Iterate over bookings involving the the selected day
            for b in bookings_involving_today():
                astart = start
                aend = b.start
                aduration = aend - astart
                start = b.end

                # Find next event if this event occupies
                # first chunk of today.
                if b.start.day != start.day:
                    continue

                # Ignore 0s periods
                if not aduration:
                    continue

                # Extract periods of time where bookings can be made.
                out.append(dtrange_as_rpc_tr(DateTimeRange(astart, aend)))
            else:
                # Return any availability periods left after all earlier bookings.
                if start < end:
                    out.append(dtrange_as_rpc_tr(DateTimeRange(start, end)))

        return ArrayTimeRangeOrError("array", out)

    @remotemethod
    async def book(self, facility: String, trange: TimeRange) -> IDOrError:
        """
        Book a facility.

        :param facility: facility name.
        :param trange: time range to book for.
        :return: booking ID or an error string.
        """
        facility = facility.value

        try:
            dtrange = rpc_tr_as_dtrange(trange)
        except ValueError:
            return IDOrError("error", String(f"invalid time range"))

        try:
            fbid = self._bt.book(facility, dtrange.as_trange())
        except (ValueError, KeyError) as e:
            return IDOrError("error", String(e.args[0]))

        return IDOrError(
            "id",
            String(
                f"{b2a_base64(facility.encode('utf-8'), newline=False).decode('utf-8')}-{fbid}"
            ),
        )

    @remotemethod
    async def modify(self, bid: String, delta: TimeDelta) -> IDOrError:
        """
        Modify a booking.

        :param bid: booking ID.
        :param delta: amount of time to shift booking for.
        :return: original booking ID or an error string.
        """
        try:
            facility, fbid = self._split_and_validate_bid(bid.value)
        except (ValueError, KeyError) as e:
            return IDOrError("error", String(e.args[0]))

        try:
            old_dtrange = self._bt.lookup(facility, fbid).as_dtrange()
            td = rpc_td_as_td(delta)
            new_dtrange = DateTimeRange(old_dtrange.start + td, old_dtrange.end + td)
        except ValueError:
            return IDOrError(
                "error", String(f"booking alteration causes booking to go out-of-week")
            )

        try:
            self._bt.modify(facility, fbid, new_dtrange.as_trange())
        except ValueError:
            return IDOrError(
                "error", String(f"altered booking conflicts with existing booking")
            )

        return IDOrError("id", bid)

    @remotemethod
    async def cancel(self, bid: String) -> IDOrError:
        """
        Cancels an existing booking.

        :param bid: booking ID.
        :return: original booking ID or an error string.
        """
        try:
            facility, fbid = self._split_and_validate_bid(bid.value)
        except ValueError as e:
            return IDOrError("error", String(e.args[0]))

        self._bt.release(facility, fbid)

        return IDOrError("id", bid)

    @remotemethod
    async def facilities(self) -> ArrayString:
        """
        Obtains the facilities available for booking.

        :return: array of strings representing the names of facilities available.
        """
        return ArrayString(map(String, sorted(self._bt.facilities)))


BookingServerProxy = generate_proxy(BookingServer)
