"""
bookingclient.py

Implementation of a client that can interact with the booking server.
"""


from serialization.numeric import u64
from serialization.derived import create_enum_type
from serialization.wellknown import Void
from rpc.common import RemoteInterface, remotemethod
from server.bookingserver import ArrayTimeRange, String


Action = create_enum_type("Action", ("CREATE", "RELEASE", "MODIFY"))


class BookingNotificationReceiver(RemoteInterface):
    @remotemethod
    def notify(
        self,
        key: u64,
        event: u64,
        action: Action,
        facility: String,
        tranges: ArrayTimeRange,
    ) -> Void:
        """
        Send a notification of a booking event.

        :param key: key provided when registering for notifications.
        :param event: event counter.
        :param action: booking action performed.
        :param facility: name of facility that was involved in the event.
        :param tranges: time ranges associated with the event.
            For ``CREATE`` and ``RELEASE`` events, only one element is passed.
            For ``MODIFY`` events, two elements are passed, where the first element is the previous
            time range of the booking, and the second element is the new time range of the booking.
        :return: nothing.
        """


#
#
# class
#
#     In[1]:
#     from rpc.proxy import generate_proxy
#
#     In[2]:
#     from server.bookingserver import BookingServer
#
#     In[3]:
#     from rpc.helpers import create_and_connect_client
#
#     In[4]:
#     from serialization.numeric import u8
#
#     In[5]:
#     from serialization.derived import String
#
#     In[6]:
#     from server.bookingserver import ArrayDayOfWeek, DayOfWeek, TimeRange
#
#     In[7]:
#     from server.bookingserver import Time
# from rpc.proxy import generate_proxy
# from server.bookingserver import BookingServer
# from rpc.helpers import create_and_connect_client
# from serialization.numeric import u8
# from serialization.derived import String
# from server.bookingserver import ArrayDayOfWeek, DayOfWeek, TimeRange
# from server.bookingserver import Time
# proxy_class = generate_proxy(BookingServer)
# c, p = await create_and_connect_client(("127.0.0.1", 5000), proxy_class)
# def print_availa(arr):
#     for a in arr:
#         print(rpc_tr_as_dtrange(a))
# from server.bookingserver import rpc_tr_as_dtrange
# days = ArrayDayOfWeek()
# days.append(DayOfWeek(DayOfWeek.VALUES.SUNDAY))
#
# print_availa((await p.query_availability(String("LKC-LT"), days)).value)
