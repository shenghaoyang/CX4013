"""
bookingclient.py

Implementation of a client that can interact with the booking server.
"""


from serialization.numeric import u64
from serialization.derived import create_enum_type
from serialization.wellknown import Void
from rpc.common import RemoteInterface, remotemethod
from rpc.proxy import generate_proxy
from server.types import ArrayTimeRange, String, rpc_tr_as_dtrange


Action = create_enum_type("Action", ("CREATE", "RELEASE", "MODIFY"))


class BookingNotificationServer(RemoteInterface):
    @remotemethod
    async def notify(
        self,
        key: u64,
        action: Action,
        facility: String,
        tranges: ArrayTimeRange,
    ) -> Void:
        """
        Send a notification of a booking event.

        :param key: key provided when registering for notifications.
        :param action: booking action performed.
        :param facility: name of facility that was involved in the event.
        :param tranges: time ranges associated with the event.
            For ``CREATE`` and ``RELEASE`` events, only one element is passed.
            For ``MODIFY`` events, two elements are passed, where the first element is the previous
            time range of the booking, and the second element is the new time range of the booking.
        :return: nothing.
        """


BookingNotificationServerProxy = generate_proxy(BookingNotificationServer)


class BookingNotificationServerImpl(BookingNotificationServer):
    def __init__(self):
        self._last_notif = ""

    @remotemethod
    async def notify(
        self, key: u64, action: Action, facility: String, tranges: ArrayTimeRange
    ) -> Void:
        key = int(key)
        action = action.value
        facility = str(facility)
        tranges = list(map(rpc_tr_as_dtrange, tranges))

        format = "%H:%M %A"
        start = tranges[0].start.strftime(format)
        end = tranges[0].end.strftime(format)

        if action == Action.VALUES.CREATE:
            self._last_notif = f"{facility} was booked from {start} to {end}."
        elif action == Action.VALUES.RELEASE:
            self._last_notif = f"{facility} is now available from {start} to {end}."
        elif action == Action.VALUES.MODIFY:
            nstart = tranges[1].start.strftime(format)
            nend = tranges[1].end.strftime(format)
            self._last_notif = (
                f"{facility} was booked from {nstart} to {nend}.\n"
                f"{facility} is now available from {start} to {end}."
            )

        return Void()

    def latest_notification(self) -> str:
        """
        Obtain a string representing the latest notification received.

        May be an empty string if no message was received yet.
        """
        return self._last_notif
