"""
notificationserver.py

Implementation of the booking notification service.
"""


from cx4013.rpc.common import RemoteInterface, remotemethod
from cx4013.rpc.proxy import generate_proxy
from cx4013.serialization.derived import create_enum_type
from cx4013.serialization.numeric import u64
from cx4013.serialization.wellknown import Void
from cx4013.server.apptypes import ArrayTimeRange, String, rpc_tr_as_dtrange

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
        """


BookingNotificationServerProxy = generate_proxy(BookingNotificationServer)


class BookingNotificationServerImpl(BookingNotificationServer):
    def __init__(self):
        self._last_notif = ""

    @remotemethod
    async def notify(
        self, key: u64, action: Action, facility: String, tranges: ArrayTimeRange
    ) -> Void:
        action = action.value
        facility = str(facility)
        tranges = list(map(rpc_tr_as_dtrange, tranges))

        if action is Action.VALUES.CREATE:
            self._last_notif = f"{facility} was booked from {tranges[0].start_str} to {tranges[0].end_str}."
        elif action is Action.VALUES.RELEASE:
            self._last_notif = f"{facility} is now available from {tranges[0].start_str} to {tranges[0].end_str}."
        elif action is Action.VALUES.MODIFY:
            self._last_notif = (
                f"{facility} was booked from {tranges[0].start_str} to {tranges[0].end_str}.\n"
                f"{facility} is now available from {tranges[1].start_str} to {tranges[1].end_str}."
            )

        return Void()

    def latest_notification(self) -> str:
        """
        Obtain a string representing the latest notification received.

        May be an empty string if no message was received yet.
        """
        return self._last_notif
