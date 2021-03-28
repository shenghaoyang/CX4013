"""
repl.py

Main client interface REPL.
"""


import datetime
from collections.abc import Sequence

from prompt_toolkit import print_formatted_text as print, HTML, PromptSession
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.document import Document
from prompt_toolkit.shortcuts import clear
from prompt_toolkit.validation import Validator, ValidationError
from tabulate import tabulate

from rpc.protocol import RPCClient
from rpc.packet import InvocationSemantics
from client.notificationserver import BookingNotificationServerImpl
from client.hooks import RandomRequestReplyDropper
from serialization.derived import String
from serialization.numeric import u8, u32, u64
from server.bookingserver import BookingServerProxy
from server.bookingtable import START_DATE
from server.apptypes import (
    DayOfWeek,
    ArrayDayOfWeek,
    rpc_tr_as_dtrange,
    dtrange_as_rpc_tr,
    DateTimeRange,
    TimeDelta,
)


class Repl:
    """
    Client REPL.
    """

    LABELS = (
        "showavail",
        "book",
        "lookup",
        "cancel",
        "modify",
        "swap",
        "list",
        "register",
        "semantics",
        "tretry",
        "csprob",
        "scprob",
        "exit",
    )
    DESCRIPTIONS = (
        "Show availability of a facility",
        "Book a facility",
        "Lookup a booking",
        "Cancel a booking",
        "Modify a booking",
        "Swap two bookings",
        "List facilities available",
        "Register for booking notifications",
        "Set RPC invocation semantics",
        "Set RPC invocation timeout and retry count",
        "Set client -> server packet loss probability",
        "Set server -> client packet loss probability",
        "Exit program",
    )
    LABEL_MAX_LENGTH = max(map(len, LABELS))

    def __init__(
        self,
        client: RPCClient,
        proxy: BookingServerProxy,
        cbport: int,
        notification_server: BookingNotificationServerImpl,
        droppers: tuple[RandomRequestReplyDropper, RandomRequestReplyDropper],
    ):
        """
        Initialize the REPL.

        :param client: RPC client.
        :param proxy: booking server proxy object.
        :param cbport: booking notification callback port.
        :param notification_server: booking notification server.
        :param droppers: tuple containing the ``(<client->server>, <server->client>)`` droppers.
        """
        self._client = client
        self._proxy = proxy
        self._cbport = cbport
        self._droppers = droppers
        self._handlers = (
            self.show_availability,
            self.book,
            self.lookup,
            self.cancel,
            self.modify,
            self.swap,
            self.list_facilities,
            self.register_notifications,
            self.set_invocation_semantics,
            self.set_timeout_retry,
            self.set_client_server_drop_probability,
            self.set_server_client_drop_probability,
            None,
        )
        self._commands = dict((l, self._handlers[i]) for i, l in enumerate(self.LABELS))

        # Known facilities cache.
        self._known_facilities = set()

        # Force periodic refresh to have the notifications be refreshed.
        # We could have the server call back into the REPL, but that's more complicated.
        self._session = PromptSession(
            bottom_toolbar=notification_server.latest_notification, refresh_interval=0.5
        )

    async def _prompt_facility(self) -> str:
        return await self._session.prompt_async(
            HTML("<i>Facility <b>name</b></i>? >>> "),
            validator=Validator.from_callable(lambda v: True),
            completer=WordCompleter(list(self._known_facilities)),
        )

    async def _prompt_dow(
        self,
        max: int = 1,
        exclude: Sequence[DayOfWeek.VALUES] = (DayOfWeek.VALUES.NEXT_MONDAY,),
    ) -> set[DayOfWeek.VALUES]:
        """
        Prompt for days of the week.

        :param max: maximum number of days to accept.
        :param exclude: day of week to exclude.
        """
        names = list(v.name for v in DayOfWeek.VALUES if v not in exclude)
        completer = WordCompleter(names, ignore_case=True)

        class DowValidator(Validator):
            def validate(self, document: Document):
                s = document.text
                days = s.split()
                if len(days) > max:
                    raise ValidationError(0, f"expecting only {max} day(s) of the week")

                for d in days:
                    if d.upper() not in names:
                        raise ValidationError(
                            s.find(d), f"{d} is not a valid day of the week"
                        )

        dows = await self._session.prompt_async(
            HTML("<i><b>Day(s)</b> of the week?</i> >>> "),
            validator=DowValidator(),
            completer=completer,
        )

        return set(DayOfWeek.VALUES[v.upper()] for v in dows.split())

    async def _prompt_time(self) -> datetime.datetime:
        def parse_time(s: str) -> datetime.datetime:
            t = START_DATE.strptime(s, "%H:%M")
            return START_DATE.replace(hour=t.hour, minute=t.minute)

        def validate_time(s: str) -> bool:
            try:
                parse_time(s)
                return True
            except ValueError:
                return False

        validator = Validator.from_callable(
            validate_time, "expecting a time in HH:MM format"
        )

        dt = parse_time(
            await self._session.prompt_async(
                HTML("<i><b>Time</b> of day (HH:MM)</i>? >>> "),
                validator=validator,
            )
        )
        dow = await self._prompt_dow(exclude=tuple())
        dt += datetime.timedelta(days=next(iter(dow)).value)

        return dt

    async def _prompt_bid(self) -> str:
        class BookingIDValidator(Validator):
            def validate(self, document: Document):
                s = document.text
                components = s.split("-")
                if len(components) != 2:
                    raise ValidationError(
                        0, f"expecting two components separated by a dash (-)"
                    )

        return await self._session.prompt_async(
            HTML("<i>Booking <b>ID</b></i>? >>> "), validator=BookingIDValidator()
        )

    async def _prompt_monitoring_time(self) -> u32:
        class U32Validator(Validator):
            def validate(self, document: Document):
                s = document.text
                try:
                    v = int(s)
                except ValueError:
                    raise ValidationError(0, "expecting an integer")

                vmin, vmax = u32.min(), u32.max()
                if not (vmin <= v <= vmax):
                    raise ValidationError(0, f"value must be within [{vmin}, {vmax}]")

        return u32(
            int(
                await self._session.prompt_async(
                    HTML("<i>Monitoring <b>Time</b>(s)</i>? >>> "),
                    validator=U32Validator(),
                )
            )
        )

    async def _prompt_timedelta(self) -> TimeDelta:
        class TimeDeltaValidator(Validator):
            def validate(self, document: Document):
                s = document.text
                components = s.split(":")
                if len(components) != 2:
                    raise ValidationError(0, f"expecting (+/-)HH:MM")

                c1, c2 = components
                pc1, pc2 = s.find(c1), s.rfind(c2)
                if c1[0] not in ("+", "-"):
                    raise ValidationError(
                        0, f"expecting +/- before HH:MM for shift direction"
                    )

                if not c1[1:].isnumeric():
                    raise ValidationError(
                        pc1, f"expecting hour shift component to be an integer"
                    )

                if not c2.isnumeric():
                    raise ValidationError(
                        pc2, f"expecting minute shift component to be an integer"
                    )

                maxval = u8.max()
                v1 = int(c1[1:])
                v2 = int(c2)
                if v1 > u8.max():
                    raise ValidationError(
                        pc1, f"expecting hour shift to be smaller than {maxval}"
                    )
                if v2 > u8.max():
                    raise ValidationError(
                        pc2, f"expecting minute shift to be smaller than {maxval}"
                    )

        ts = await self._session.prompt_async(
            HTML("<i>Time shift (+/-HH:MM)</i>? >>> "), validator=TimeDeltaValidator()
        )

        neg = u8(1) if ts[0] == "-" else u8(0)
        hours, minutes = map(u8, map(int, ts[1:].split(":")))
        return TimeDelta(hours=hours, minutes=minutes, negative=neg)

    async def _prompt_probability(self) -> float:
        def probability_validator(s: str) -> bool:
            try:
                v = float(s)
                return 0 <= v <= 1
            except ValueError:
                return False

        validator = Validator.from_callable(
            probability_validator, "expecting a real number within [0, 1]"
        )
        return float(
            await self._session.prompt_async(
                HTML("<i><b>Probability</b></i>? >>> "), validator=validator
            )
        )

    def _print_error(self, s: str):
        clear()
        print(HTML(f"<ansired>ERROR:</ansired> <u>{s}</u>"))

    async def show_availability(self):
        name = await self._prompt_facility()
        dows = await self._prompt_dow(7)
        dows = ArrayDayOfWeek(map(DayOfWeek, dows))

        res = await self._proxy.query_availability(String(name), dows)

        if "error" in res:
            self._print_error(str(res.value))
            return

        clear()
        print(
            HTML(
                f"<b>Availability periods for</b> <ansigreen><u>{name}</u></ansigreen>:"
            )
        )

        def formatted_gen():
            for tr in res.value:
                dtrange = rpc_tr_as_dtrange(tr)
                yield (
                    DayOfWeek.VALUES(dtrange.start.weekday()).name,
                    dtrange.start_str,
                    dtrange.end_str,
                )

        table_data = tuple(formatted_gen())
        print(
            tabulate(
                table_data,
                headers=("Weekday", "Start", "End"),
                stralign="left",
                tablefmt="psql",
            )
        )
        self._known_facilities.add(name)

    async def book(self):
        name = await self._prompt_facility()
        print(HTML(f"<u>Enter <b>start</b> time</u>:"))
        start = await self._prompt_time()
        print(HTML(f"<u>Enter <b>end</b> time</u>"))
        end = await self._prompt_time()

        rpc_tr = dtrange_as_rpc_tr(DateTimeRange(start, end))
        res = await self._proxy.book(String(name), rpc_tr)

        if "error" in res:
            self._print_error(str(res.value))
            return

        clear()
        print(
            HTML(
                f"<ansigreen>Successfully</ansigreen> booked {name}."
                f" Confirmation ID: <b><u>{res.value}</u></b>."
            )
        )

    async def lookup(self):
        bid = await self._prompt_bid()

        res = await self._proxy.lookup(String(bid))

        if "error" in res:
            self._print_error(str(res.value))
            return

        booking = res["booking"]
        facility = str(booking["facility"])
        dtrange = rpc_tr_as_dtrange(booking["trange"])

        clear()
        print(
            HTML(
                f"Booking <b><u>{bid}</u></b> <ansigreen>exists</ansigreen> and corresponds to the time period\n"
                f"<i>{dtrange}</i> for facility <b>{facility}</b>."
            )
        )

    async def swap(self):
        print(HTML(f"<u>Enter <b>first</b> booking</u>:"))
        bid1 = await self._prompt_bid()
        print(HTML(f"<u>Enter <b>second</b> booking</u>"))
        bid2 = await self._prompt_bid()

        res = await self._proxy.swap(String(bid1), String(bid2))

        if "error" in res:
            self._print_error(str(res.value))
            return

        clear()
        print(
            HTML(
                f"<ansigreen>Successfully</ansigreen> swapped bookings <b><u>{bid1}</u></b> and <b><u>{bid2}</u></b>."
            )
        )

    async def modify(self):
        bid = await self._prompt_bid()
        shift = await self._prompt_timedelta()

        res = await self._proxy.modify(String(bid), shift)

        if "error" in res:
            self._print_error(str(res.value))
            return

        clear()
        print(
            HTML(
                f"<ansigreen>Successfully</ansigreen> modified booking"
                f" <b><u>{res.value}</u></b>."
            )
        )

    async def cancel(self):
        bid = await self._prompt_bid()

        res = await self._proxy.cancel(String(bid))

        if "error" in res:
            self._print_error(str(res.value))
            return

        clear()
        print(
            HTML(f"<ansigreen>Successfully</ansigreen> canceled booking <u>{bid}</u>")
        )

    async def list_facilities(self):
        res = await self._proxy.facilities()

        facilities = tuple(map(str, res))
        clear()
        print(
            tabulate(
                tuple((f,) for f in facilities),
                headers=("Available facilities",),
                tablefmt="psql",
            )
        )

        self._known_facilities.update(facilities)

    async def register_notifications(self):
        facility = await self._prompt_facility()
        monitoring_time = await self._prompt_monitoring_time()

        res = await self._proxy.register_notification(
            u32(self._cbport), u64(0), String(facility), monitoring_time
        )

        if "error" in res:
            self._print_error(res.value)
            return

        self._known_facilities.add(facility)

        clear()
        print(
            HTML(
                f"<ansigreen>Successfully</ansigreen> registered for notifications regarding <u>{facility}</u>."
            )
        )

    async def set_invocation_semantics(self):
        names = list(v.name for v in InvocationSemantics)
        completer = WordCompleter(names, ignore_case=True)

        class InvocationSemanticsValidator(Validator):
            def validate(self, document: Document):
                s = document.text
                if s not in names:
                    raise ValidationError(0, f"expecting one of {names}")

        self._client.semantics = InvocationSemantics[
            await self._session.prompt_async(
                HTML("<i><b>Semantics</b>?</i> >>> "),
                completer=completer,
                validator=InvocationSemanticsValidator(),
            )
        ]

    async def set_timeout_retry(self):
        class TimeoutValidator(Validator):
            def validate(self, document: Document):
                s = document.text

                if s == "None":
                    return

                try:
                    v = float(s)
                except ValueError:
                    raise ValidationError(
                        0, "expecting a non-negative real number or 'None'"
                    )

                if v < 0:
                    raise ValidationError(0, "expecting a non-negative timeout")

        timeout = await self._session.prompt_async(
            HTML("<i><b>Timeout</b> (seconds or None)?</i> >>> "),
            validator=TimeoutValidator(),
        )

        timeout = None if timeout == "None" else float(timeout)

        class RetriesValidator(Validator):
            def validate(self, document: Document):
                s = document.text

                try:
                    v = int(s)
                except ValueError:
                    raise ValidationError(0, "expecting a non-negative integer")

                if v < 0:
                    raise ValidationError(0, "expecting a non-negative retry count")

        retries = int(
            await self._session.prompt_async(
                HTML("<i><b>Retries</b>?</i> >>> "), validator=RetriesValidator()
            )
        )

        self._client.timeout = timeout
        self._client.retries = retries

    async def set_client_server_drop_probability(self):
        print(
            HTML(f"Current probability is <ansired>{self._droppers[0].prob}</ansired>.")
        )
        self._droppers[0].prob = await self._prompt_probability()

    async def set_server_client_drop_probability(self):
        print(
            HTML(f"Current probability is <ansired>{self._droppers[1].prob}</ansired>.")
        )
        self._droppers[1].prob = await self._prompt_probability()

    async def run(self):
        completer = WordCompleter(list(self.LABELS))
        validator = Validator.from_callable(
            self._commands.__contains__, "not a valid command"
        )

        while True:
            try:
                print(HTML("<u>Status:</u>"))
                print(
                    HTML(
                        f"<b>Client -> server</b> drop probability: <ansired>{self._droppers[0].prob}</ansired>\n"
                        f"<b>Server -> client</b> drop probability: <ansired>{self._droppers[1].prob}</ansired>"
                    )
                )
                print(
                    HTML(
                        f"RPC invocation semantics: <i>{self._client.semantics.name}</i>"
                    )
                )
                print(
                    HTML(
                        f"RPC timeout: <i>{self._client.timeout}</i> (s), retries: <i>{self._client.retries}</i>"
                    )
                )
                print(HTML("\n<u>Commands:</u>"))
                for i, command in enumerate(self.LABELS):
                    print(
                        HTML(
                            f"<ansigreen><b>{command.rjust(self.LABEL_MAX_LENGTH)}</b></ansigreen>: {self.DESCRIPTIONS[i]}"
                        )
                    )
                print("")

                cmd = await self._session.prompt_async(
                    HTML("<i>command?</i> >>> "),
                    completer=completer,
                    validator=validator,
                )

                if cmd == self.LABELS[-1]:
                    return

                await self._commands[cmd]()
            except KeyboardInterrupt:
                continue
            except EOFError:
                return
