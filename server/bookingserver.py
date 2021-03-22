"""
bookingserver.py

Implementation of a remote class that can be used to interact with the
booking system.
"""


import asyncio
from binascii import a2b_base64, b2a_base64
from datetime import timedelta
from typing import cast
from dataclasses import dataclass
from collections.abc import Sequence
from server.bookingtable import Table, DateTimeRange, START_DATE
from server.types import (
    ArrayString,
    ArrayDayOfWeek,
    ArrayTimeRangeOrError,
    Booking,
    BookingOrError,
    TimeRange,
    IDOrError,
    TimeDelta,
    DayOfWeek,
    ArrayTimeRange,
    dtrange_as_rpc_tr,
    rpc_tr_as_dtrange,
    rpc_td_as_td,
)
from client.notificationserver import BookingNotificationServerProxy, Action
from serialization.derived import String
from serialization.wellknown import VoidOrError
from serialization.numeric import u32, u64
from rpc.common import RemoteInterface, remotemethod
from rpc.proxy import generate_proxy
from rpc.protocol import RPCClient, AddressType
from rpc.helpers import create_and_connect_client


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
    async def lookup(self, bid: String) -> BookingOrError:
        """
        Look up information about an existing booking.

        :param bid: booking ID.
        :return: booking information or an error string.
        """

    @remotemethod
    async def facilities(self) -> ArrayString:
        """
        Obtains the facilities available for booking.

        :return: array of strings representing the names of facilities available.
        """

    @remotemethod
    async def swap(self, b1: String, b2: String) -> VoidOrError:
        """
        Swaps two bookings for the same facility.

        :param b1:
        :param b2:
        :return:
        """

    @remotemethod
    async def register_notification(
        self, port: u32, key: u64, facility: String, seconds: u32
    ) -> VoidOrError:
        """
        Register for notifications regarding bookings made on a particular
        facility.

        :param port: port to send notifications to.
            A new connection will be made for each registration.
        :param key: key that will be provided with each notification.
        :param facility: facility to watch.
        :param seconds: number of seconds to watch facility for.
        :return: error message on failure to register, ``Void`` otherwise.
        """


@dataclass(frozen=True)
class NotificationServer:
    """
    Dataclass representing a connected notification server.
    """

    # Facility that this server is interested in.
    facility: str
    # Key to return to server.
    key: int
    # RPC client used to manage the connection.
    client: RPCClient
    # Proxy used for RPC notifications.
    proxy: BookingNotificationServerProxy
    # Task used to gate the lifetime of this connection.
    task: asyncio.Task


class BookingServerImpl(BookingServer):
    def __init__(
        self, bt: Table, caddr: AddressType, shared_with: set["BookingServerImpl"]
    ):
        """
        Create a new booking server.

        :param bt: booking table to refer to.
        :param caddr: address of client.
        :param shared_with: booking servers sharing access to the same table.
        """
        self._bt = bt
        self._caddr = caddr
        self._shared_with = shared_with
        self._notification_servers: set[NotificationServer] = set()
        # todo lock database against concurrent modification.
        # might not be required since we don't come across any async
        # preemption points during modification of the database

        self._shared_with.add(self)

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
        facility = facility.value

        try:
            dtrange = rpc_tr_as_dtrange(trange)
        except ValueError:
            return IDOrError("error", String(f"invalid time range"))

        try:
            fbid = self._bt.book(facility, dtrange.as_trange())
        except (ValueError, KeyError) as e:
            return IDOrError("error", String(e.args[0]))

        tasks = [
            asyncio.create_task(
                s.send_notification(Action.VALUES.CREATE, facility, (dtrange,))
            )
            for s in self._shared_with
        ]

        await asyncio.wait(tasks)

        return IDOrError(
            "id",
            String(
                f"{b2a_base64(facility.encode('utf-8'), newline=False).decode('utf-8')}-{fbid}"
            ),
        )

    @remotemethod
    async def modify(self, bid: String, delta: TimeDelta) -> IDOrError:
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

        tasks = [
            asyncio.create_task(
                s.send_notification(
                    Action.VALUES.MODIFY, facility, (old_dtrange, new_dtrange)
                )
            )
            for s in self._shared_with
        ]

        await asyncio.wait(tasks)

        return IDOrError("id", bid)

    @remotemethod
    async def cancel(self, bid: String) -> IDOrError:
        try:
            facility, fbid = self._split_and_validate_bid(bid.value)
        except ValueError as e:
            return IDOrError("error", String(e.args[0]))

        trange = self._bt.lookup(facility, fbid)
        self._bt.release(facility, fbid)

        tasks = [
            asyncio.create_task(
                s.send_notification(
                    Action.VALUES.RELEASE, facility, (trange.as_dtrange(),)
                )
            )
            for s in self._shared_with
        ]

        await asyncio.wait(tasks)

        return IDOrError("id", bid)

    @remotemethod
    async def facilities(self) -> ArrayString:
        return ArrayString(map(String, sorted(self._bt.facilities)))

    @remotemethod
    async def register_notification(
        self, port: u32, key: u64, facility: String, seconds: u32
    ) -> VoidOrError:
        if port.value > 0xFFFF:
            return VoidOrError("error", String("invalid port number"))

        facility = str(facility)
        if facility not in self._bt.facilities:
            return VoidOrError("error", String(f"facility {facility} does not exist"))

        try:
            saddr = (self._caddr[0], port.value)
            c, p = await asyncio.wait_for(
                create_and_connect_client(saddr, BookingNotificationServerProxy), 10
            )
        except asyncio.TimeoutError:
            return VoidOrError("error", String("timeout while connecting to server"))

        # Notifications are best-effort only.
        c.timeout = 0
        c.retries = 0
        key = int(key)
        # Create task to expire the connection to the notification server after the
        # specified monitoring interval.
        tsk = asyncio.create_task(asyncio.sleep(int(seconds)))
        ns = NotificationServer(client=c, proxy=p, facility=facility, key=key, task=tsk)

        self._notification_servers.add(ns)

        def callback(_: asyncio.Task):
            ns.client.close()
            self._notification_servers.remove(ns)

        tsk.add_done_callback(callback)

        return VoidOrError("void")

    @remotemethod
    async def lookup(self, bid: String) -> BookingOrError:
        try:
            facility, fbid = self._split_and_validate_bid(bid.value)
        except ValueError as e:
            return BookingOrError("error", String(e.args[0]))

        trange = self._bt.lookup(facility, fbid)

        return BookingOrError(
            "booking",
            Booking(
                trange=dtrange_as_rpc_tr(trange.as_dtrange()), facility=String(facility)
            ),
        )

    @remotemethod
    async def swap(self, b1: String, b2: String) -> VoidOrError:
        try:
            f1, fbid1 = self._split_and_validate_bid(b1.value)
            f2, fbid2 = self._split_and_validate_bid(b2.value)
        except ValueError as e:
            return VoidOrError("error", String(e.args[0]))

        if f1 != f2:
            return VoidOrError(
                "error", String("bookings are not for the same facility")
            )

        self._bt.swap(f1, (fbid1, fbid2))

        return VoidOrError("void")

    async def send_notification(
        self, action: Action.VALUES, facility: str, dtranges: Sequence[DateTimeRange]
    ):
        """
        Send notifications to notification servers regarding booking actions.

        :param facility: facility name.
        :param action: booking action performed.
        :param dtranges: time ranges involved.
        """

        rpc_dtranges = ArrayTimeRange(map(dtrange_as_rpc_tr, dtranges))
        rpc_action = Action(action)
        rpc_facility = String(facility)
        tasks = dict(
            (
                asyncio.create_task(
                    s.proxy.notify(u64(s.key), rpc_action, rpc_facility, rpc_dtranges)
                ),
                s,
            )
            for s in self._notification_servers
            if s.facility == facility
        )

        # We use wait() here because we don't want the "don't abandon" behavior
        if tasks:
            await asyncio.wait(tasks)

        # Disconnect notification servers that can't be contacted.
        for task, ns in tasks.items():
            # Ignore invocation timeouts because notifications are best-effort only.
            # The connection will eventually be closed due to the inactivity timeouts.
            if (task.exception() is not None) and (
                not isinstance(task.exception(), asyncio.TimeoutError)
            ):
                ns.task.cancel()

    def handle_disconnect(self):
        # cancel all the notification tasks and remove self from shared set.
        self._shared_with.remove(self)
        for s in self._notification_servers:
            s.task.cancel()


BookingServerProxy = generate_proxy(BookingServer)
