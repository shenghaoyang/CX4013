"""
main.py

Main client interface.
"""


import asyncio
import sys
import argparse
import datetime

from prompt_toolkit.document import Document
from tabulate import tabulate
from prompt_toolkit import print_formatted_text as print, HTML, PromptSession
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.validation import Validator, ValidationError
from prompt_toolkit.shortcuts import clear
from serialization.derived import String
from serialization.numeric import u8
from rpc.common import DEFAULT_PORT
from server.bookingtable import START_DATE
from server.bookingserver import (
    BookingServerProxy,
    DayOfWeek,
    ArrayDayOfWeek,
    rpc_tr_as_dtrange,
    dtrange_as_rpc_tr,
    DateTimeRange,
    TimeDelta,
)
from rpc.helpers import create_and_connect_client
from collections.abc import Sequence


class Repl:
    """
    Client REPL.
    """

    LABELS = ("showavail", "book", "cancel", "modify", "list", "exit")
    DESCRIPTIONS = (
        "Show availability of a facility",
        "Book a facility",
        "Cancel a booking",
        "Modify a booking",
        "List facilities available",
        "Exit program",
    )

    def __init__(self, proxy: BookingServerProxy):
        """
        Initialize the REPL.

        :param proxy: booking server proxy object.
        """
        # mlen = max(map(len, self.LABELS))
        # self._labels = tuple(map(lambda s: s.rjust(mlen), self.LABELS))
        self._proxy = proxy
        self._handlers = (
            self.show_availability,
            self.book,
            self.cancel,
            self.modify,
            self.list_facilities,
            None,
        )
        self._commands = dict((l, self._handlers[i]) for i, l in enumerate(self.LABELS))
        self._known_facilities = set()
        self._session = PromptSession()

    async def _prompt_facility(self) -> str:
        return await self._session.prompt_async(
            HTML("<i>Facility <b>name</b></i>? >>> "),
            validator=Validator.from_callable(lambda v: True),
            completer=WordCompleter(list(self._known_facilities)),
        )

    async def _prompt_dow(self, max: int = 1) -> set[DayOfWeek.VALUES]:
        """
        Prompt for days of the week.

        :param max: maximum number of days to accept.
        """
        names = set(v.name for v in DayOfWeek.VALUES)
        completer = WordCompleter(list(names), ignore_case=True)

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
        dow = await self._prompt_dow()
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

                for i, component in enumerate(components):
                    if not component.isnumeric():
                        raise ValidationError(
                            s.rfind(component) if not i else s.find(component),
                            f"{component} is not an integer",
                        )

        return await self._session.prompt_async(
            HTML("<i>Booking <b>ID</b></i>? >>> "), validator=BookingIDValidator()
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
        date_format = "%H:%M %A"

        def formatted_gen():
            for tr in res.value:
                dtrange = rpc_tr_as_dtrange(tr)
                yield (
                    DayOfWeek.VALUES(dtrange.start.weekday()).name,
                    dtrange.start.strftime(date_format),
                    dtrange.end.strftime(date_format),
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
        print(HTML(f"Enter <b>start</b> time"))
        start = await self._prompt_time()
        print(HTML(f"Enter <b>end</b> time"))
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

    async def run(self):
        completer = WordCompleter(list(self.LABELS))
        validator = Validator.from_callable(
            self._commands.__contains__, "not a valid command"
        )

        while True:
            print(HTML("<b>Available commands:</b>\n"))
            for i, command in enumerate(self.LABELS):
                print(HTML(f"<u><b>{command}</b></u>: {self.DESCRIPTIONS[i]}"))
            print("")

            cmd = await self._session.prompt_async(
                HTML("<i>command?</i> >>> "), completer=completer, validator=validator
            )

            if cmd == "exit":
                return

            await self._commands[cmd]()


async def main(args: Sequence[str]) -> int:
    """
    Main client application entry point.

    :param args: program arguments a-la sys.argv.
    """
    parser = argparse.ArgumentParser(
        description="CALRPC client application.", exit_on_error=False
    )
    parser.add_argument("SERVER", help="IPv4 address of server")
    parser.add_argument(
        "--sport", help="port to connect to on server", type=int, default=DEFAULT_PORT
    )
    parser.add_argument(
        "--caddr",
        help="IPv4 address used for listening to callbacks",
        default="0.0.0.0",
    )
    parser.add_argument(
        "--cport",
        help="port to listen for callback connections on",
        type=int,
        default=DEFAULT_PORT + 1,
    )
    parser.add_argument(
        "--ctimeout", help="initial connection timeout (seconds)", type=int, default=10
    )

    # Parse the arguments.
    try:
        args = parser.parse_args(args[1:])
    except argparse.ArgumentError:
        return 1

    # Attempt to connect to the server.
    print(HTML(r"<b>Connecting to RPC server</b>"))
    try:
        c, p = await asyncio.wait_for(
            create_and_connect_client((args.SERVER, args.sport), BookingServerProxy),
            args.ctimeout,
        )
        print(HTML(r"<b>Connected</b>"))
    except asyncio.TimeoutError:
        print(HTML(r"<b><ansired>Could not connect to RPC server</ansired></b>"))
        return 1

    clear()
    repl = Repl(p)
    await repl.run()

    c.close()


if __name__ == "__main__":
    exit(asyncio.run(main(sys.argv)))
